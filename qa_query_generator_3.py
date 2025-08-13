import pandas as pd
from google.cloud import bigquery


class QAQueryGeneratorFromColumns:
    def __init__(self, project_id: str, source_table: str, output_table: str):
        """
        Initialize with:
          - project_id: GCP project (BigQuery client scope)
          - source_table: table/view with columns metadata (table_catalog, table_schema, table_name, column_name)
          - output_table: destination table to store the QA plan rows
        """
        self.project_id = project_id
        self.source_table = source_table
        self.output_table = output_table
        self.client = bigquery.Client(project=project_id)

    def load_column_metadata(self) -> pd.DataFrame:
        """
        Load columns metadata from BigQuery.
        Expected schema in source_table:
          table_catalog, table_schema, table_name, column_name
        """
        query = f"""
            SELECT table_catalog, table_schema, table_name, column_name
            FROM `{self.project_id}.{self.source_table}`
        """
        print("[INFO] Loading column metadata from BigQuery...")
        return self.client.query(query).to_dataframe(create_bqstorage_client=False)

    def group_columns_by_table(self, df: pd.DataFrame) -> dict:
        """
        Group column names by (schema, table). Returns dict: (schema, table) -> [columns...]
        """
        grouped = df.groupby(['table_schema', 'table_name'])['column_name'].apply(list)
        return grouped.to_dict()

    @staticmethod
    def sanitize_column(col: str) -> str:
        """
        Wrap a column name with SQL Server brackets.
        """
        return f"[{col}]"

    def build_count_query(self, catalog: str, schema: str, table: str, full_table: str) -> dict:
        """
        COUNT(*) over the fully qualified table.
        Output format: single_row_scalar
        """
        qualified = f"[{catalog}].[{schema}].[{table}]"
        return {
            "table_name": full_table,
            "check_type": "count_total_rows",
            "query_text": f"SELECT COUNT(*) AS total_rows FROM {qualified}",
            "expected_output_format": "single_row_scalar"
        }

    def build_nulls_query(self, catalog: str, schema: str, table: str, full_table: str, columns: list) -> dict:
        """
        One-row output with one metric per column: number of NULLs.
        Output format: single_row_scalar
        """
        qualified = f"[{catalog}].[{schema}].[{table}]"
        cols = [self.sanitize_column(c) for c in columns]
        null_checks = ",\n    ".join(
            f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS nulls_{c[1:-1]}" for c in cols
        )
        return {
            "table_name": full_table,
            "check_type": "nulls_per_column",
            "query_text": f"SELECT\n    {null_checks}\nFROM {qualified}",
            "expected_output_format": "single_row_scalar"
        }

    def build_hashcheck_query(self, catalog: str, schema: str, table: str, full_table: str, columns: list) -> dict:
        """
        Row-level hash based on all columns casted to NVARCHAR(MAX) and concatenated with '|'.
        Output format: row_level
        """
        qualified = f"[{catalog}].[{schema}].[{table}]"
        cols = [self.sanitize_column(c) for c in columns]
        concat_expr = ", ".join(f"CAST({c} AS NVARCHAR(MAX))" for c in cols)
        query = f"""
            SELECT *,
                   CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT_WS('|', {concat_expr})), 2) AS hashcheck
            FROM {qualified}
        """.strip()
        return {
            "table_name": full_table,
            "check_type": "hashcheck",
            "query_text": query,
            "expected_output_format": "row_level"
        }

    def build_duplicates_query(self, catalog: str, schema: str, table: str, full_table: str, column: str) -> dict:
        """
        Detect duplicates on a given column (first column used by default upstream).
        Output format: multi_row_summary
        """
        qualified = f"[{catalog}].[{schema}].[{table}]"
        col = self.sanitize_column(column)
        query = f"""
            SELECT {col}, COUNT(*) AS cnt
            FROM {qualified}
            GROUP BY {col}
            HAVING COUNT(*) > 1
        """.strip()
        return {
            "table_name": full_table,
            "check_type": "duplicates_primary_key",
            "query_text": query,
            "expected_output_format": "multi_row_summary"
        }

    def generate_all_queries(self) -> pd.DataFrame:
        """
        Build the QA plan rows for each table:
          - count_total_rows
          - nulls_per_column (all columns in one row)
          - hashcheck (row-level hash of all columns)
          - duplicates_primary_key (based on the first column if present)
        NOTE: value_dist_<column> was intentionally removed.
        """
        df = self.load_column_metadata()
        grouped = df.groupby(['table_catalog', 'table_schema', 'table_name'])['column_name'].apply(list)
        grouped = grouped.reset_index(name='columns')

        all_queries = []
        print("[INFO] Generating QA queries (count, nulls, hashcheck, duplicates)...")

        for _, row in grouped.iterrows():
            catalog = row['table_catalog']
            schema = row['table_schema']
            table = row['table_name']
            columns = row['columns']

            # Keep this human-friendly display (schema.table)
            full_table = f"{schema}.{table}"

            # 1) Count total rows
            all_queries.append({
                **self.build_count_query(catalog, schema, table, full_table),
                "table_catalog": catalog,
                "table_schema": schema,
                "table_name": table
            })

            # 2) Nulls per column
            all_queries.append({
                **self.build_nulls_query(catalog, schema, table, full_table, columns),
                "table_catalog": catalog,
                "table_schema": schema,
                "table_name": table
            })

            # 3) Row-level hash
            all_queries.append({
                **self.build_hashcheck_query(catalog, schema, table, full_table, columns),
                "table_catalog": catalog,
                "table_schema": schema,
                "table_name": table
            })

            # 4) Duplicates on first column (only if we have columns)
            if columns:
                all_queries.append({
                    **self.build_duplicates_query(catalog, schema, table, full_table, columns[0]),
                    "table_catalog": catalog,
                    "table_schema": schema,
                    "table_name": table
                })

        print(f"[INFO] Total QA queries generated: {len(all_queries)}")
        return pd.DataFrame(all_queries)

    def upload_to_bigquery(self, df: pd.DataFrame):
        """
        Load the generated plan to BigQuery (WRITE_TRUNCATE).
        """
        table_ref = f"{self.project_id}.{self.output_table}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        print(f"[INFO] Uploading {len(df)} QA queries to {table_ref} ...")
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"[INFO] Uploaded {len(df)} QA queries to {table_ref}")
