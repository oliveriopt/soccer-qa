import pandas as pd
from google.cloud import bigquery


class QAQueryGeneratorFromColumnsMSSQL:
    """
    Build QA queries in pure T-SQL to be executed on SQL Server via pyodbc.
    Output schema (per row):
      - table_name               (e.g., "schema.table" for easy reading)
      - table_catalog            (database/catalog in SQL Server)
      - table_schema
      - query_text               (T-SQL string)
      - check_type               ("count_total_rows" | "nulls_per_column" | optional others)
      - expected_output_format   ("single_row_scalar")
    """

    def __init__(
        self,
        project_id: str,
        source_table: str,
        output_table: str,
        include_duplicates_pk: bool = False,
        include_hashcheck: bool = False,
        pk_column: str = None,   # only used if include_duplicates_pk=True
    ):
        """
        Args:
          project_id:      GCP project that hosts the BigQuery metadata table.
          source_table:    BigQuery table with column metadata
                           (must include: table_catalog, table_schema, table_name, column_name).
          output_table:    BigQuery destination table to store the QA plan.
          include_duplicates_pk: include 'duplicates_primary_key' T-SQL check (optional).
          include_hashcheck:     include 'hashcheck' T-SQL check (optional).
          pk_column:             primary key column name (SQL Server name); only for duplicates check.
        """
        self.project_id = project_id
        self.source_table = source_table
        self.output_table = output_table
        self.include_duplicates_pk = include_duplicates_pk
        self.include_hashcheck = include_hashcheck
        self.pk_column = pk_column
        self.client = bigquery.Client(project=project_id)

    # -------- BigQuery I/O --------
    def load_column_metadata(self) -> pd.DataFrame:
        """Load column metadata from BigQuery (expects catalog/schema/table/column)."""
        query = f"""
            SELECT table_catalog, table_schema, table_name, column_name
            FROM `{self.project_id}.{self.source_table}`
        """
        return self.client.query(query).to_dataframe(create_bqstorage_client=False)

    def upload_to_bigquery(self, df: pd.DataFrame):
        """Upsert/replace the QA plan table in BigQuery."""
        table_ref = f"{self.project_id}.{self.output_table}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Uploaded {len(df)} QA queries to {table_ref}")

    # -------- helpers to build T-SQL --------
    @staticmethod
    def _q_table(catalog: str, schema: str, table: str) -> str:
        """Return fully qualified T-SQL table: [catalog].[schema].[table]."""
        return f"[{catalog}].[{schema}].[{table}]"

    @staticmethod
    def _col(col: str) -> str:
        """Bracket-quote a column for T-SQL."""
        return f"[{col}]"

    def _build_count_query(self, catalog: str, schema: str, table: str, display_name: str) -> dict:
        qualified = self._q_table(catalog, schema, table)
        sql = f"SELECT COUNT(*) AS total_rows FROM {qualified};"
        return {
            "table_name": display_name,                 # e.g., "schema.table"
            "table_catalog": catalog,
            "table_schema": schema,
            "check_type": "count_total_rows",
            "query_text": sql,
            "expected_output_format": "single_row_scalar",
        }

    def _build_nulls_query(self, catalog: str, schema: str, table: str, display_name: str, columns: list) -> dict:
        qualified = self._q_table(catalog, schema, table)
        # Build: SUM(CASE WHEN [Col] IS NULL THEN 1 ELSE 0 END) AS nulls_Col
        parts = []
        for c in columns:
            c_br = self._col(c)
            alias = f"nulls_{c}"
            parts.append(f"SUM(CASE WHEN {c_br} IS NULL THEN 1 ELSE 0 END) AS {alias}")
        body = ",\n    ".join(parts)
        sql = f"SELECT\n    {body}\nFROM {qualified};"
        return {
            "table_name": display_name,
            "table_catalog": catalog,
            "table_schema": schema,
            "check_type": "nulls_per_column",
            "query_text": sql,
            "expected_output_format": "single_row_scalar",
        }

    def _build_duplicates_query(self, catalog: str, schema: str, table: str, display_name: str) -> dict:
        """Optional: duplicates by PK column. Returns rows >1 per value of PK."""
        if not self.pk_column:
            raise ValueError("pk_column must be provided to build duplicates_primary_key.")
        qualified = self._q_table(catalog, schema, table)
        pk = self._col(self.pk_column)
        sql = f"""
            SELECT {pk} AS pk_value, COUNT(*) AS cnt
            FROM {qualified}
            GROUP BY {pk}
            HAVING COUNT(*) > 1;
        """.strip()
        return {
            "table_name": display_name,
            "table_catalog": catalog,
            "table_schema": schema,
            "check_type": "duplicates_primary_key",
            "query_text": sql,
            "expected_output_format": "multi_row_summary",
        }

    def _build_hashcheck_query(self, catalog: str, schema: str, table: str, display_name: str, columns: list) -> dict:
        """Optional: per-row SHA-256 hash across all columns (row-level output)."""
        qualified = self._q_table(catalog, schema, table)
        casts = ", ".join([f"CAST({self._col(c)} AS NVARCHAR(MAX))" for c in columns])
        sql = f"""
            SELECT *,
                   CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT_WS('|', {casts})), 2) AS hashcheck
            FROM {qualified};
        """.strip()
        return {
            "table_name": display_name,
            "table_catalog": catalog,
            "table_schema": schema,
            "check_type": "hashcheck",
            "query_text": sql,
            "expected_output_format": "row_level",
        }

    # -------- main orchestration --------
    def generate_all_queries(self) -> pd.DataFrame:
        """
        Build the QA plan dataframe with T-SQL queries for:
          - count_total_rows
          - nulls_per_column
        Optionally:
          - duplicates_primary_key (if include_duplicates_pk=True and pk_column provided)
          - hashcheck (if include_hashcheck=True)
        """
        df = self.load_column_metadata()
        # group columns by table
        grouped = (
            df.groupby(['table_catalog', 'table_schema', 'table_name'])['column_name']
              .apply(list)
              .reset_index(name='columns')
        )

        out_rows = []
        for _, row in grouped.iterrows():
            catalog = row['table_catalog']
            schema = row['table_schema']
            table = row['table_name']
            columns = row['columns']

            # Display name stays "schema.table" (handy for logs/filters)
            display_name = f"{schema}.{table}"

            # 1) count_total_rows
            out_rows.append(self._build_count_query(catalog, schema, table, display_name))

            # 2) nulls_per_column (only real columns from metadata)
            out_rows.append(self._build_nulls_query(catalog, schema, table, display_name, columns))

            # Optional 3) duplicates by PK
            if self.include_duplicates_pk:
                out_rows.append(self._build_duplicates_query(catalog, schema, table, display_name))

            # Optional 4) row-level hash
            if self.include_hashcheck:
                out_rows.append(self._build_hashcheck_query(catalog, schema, table, display_name, columns))

        return pd.DataFrame(out_rows)
