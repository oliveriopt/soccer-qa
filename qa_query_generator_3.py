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

    @staticmethod
    def sanitize_column(col: str) -> str:
        """Wrap a column name with SQL Server brackets."""
        return f"[{col}]"

    # --- Query builders (return: check_type, query_text, expected_output_format) ---

    def build_count_query(self, catalog: str, schema: str, table: str) -> dict:
        """COUNT(*) over the fully qualified table."""
        qualified = f"[{catalog}].[{schema}].[{table}]"
        return {
            "check_type": "count_total_rows",
            "query_text": f"SELECT COUNT(*) AS total_rows FROM {qualified}",
            "expected_output_format": "single_row_scalar",
        }

    def build_nulls_query(self, catalog: str, schema: str, table: str, columns: list) -> dict:
        """
        One-row output with one metric per column: number of NULLs.
        Single row with many columns: nulls_<col>.
        """
        qualified = f"[{catalog}].[{schema}].[{table}]"
        cols = [self.sanitize_column(c) for c in columns]
        null_checks = ",\n    ".join(
            f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS nulls_{c[1:-1]}" for c in cols
        ) if cols else "COUNT(*) AS no_columns_detected"
        return {
            "check_type": "nulls_per_column",
            "query_text": f"SELECT\n    {null_checks}\nFROM {qualified}",
            "expected_output_format": "single_row_scalar",
        }

    def build_hashcheck_query(self, catalog: str, schema: str, table: str, columns: list) -> dict:
        """
        Row-level hash based on all columns cast to NVARCHAR(MAX) and concatenated with '|'.
        Potentially heavy for very large tables.
        """
        qualified = f"[{catalog}].[{schema}].[{table}]"
        cols = [self.sanitize_column(c) for c in columns]
        if cols:
            concat_expr = ", ".join(f"CAST({c} AS NVARCHAR(MAX))" for c in cols)
        else:
            # Fallback to a constant if no columns (keeps SQL valid)
            concat_expr = "'__NO_COLUMNS__'"
        query = f"""
            SELECT *,
                   CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT_WS('|', {concat_expr})), 2) AS hashcheck
            FROM {qualified}
        """.strip()
        return {
            "check_type": "hashcheck",
            "query_text": query,
            "expected_output_format": "row_level",
        }

    def build_duplicates_query(self, catalog: str, schema: str, table: str, columns: list) -> dict:
        """
        Detect duplicates on the first column if present.
        """
        qualified = f"[{catalog}].[{schema}].[{table}]"
        if columns:
            col = self.sanitize_column(columns[0])
            query = f"""
                SELECT {col} AS candidate_key, COUNT(*) AS cnt
                FROM {qualified}
                GROUP BY {col}
                HAVING COUNT(*) > 1
            """.strip()
        else:
            query = f"SELECT 'NO_COLUMNS' AS candidate_key, 0 AS cnt"
        return {
            "check_type": "duplicates_primary_key",
            "query_text": query,
            "expected_output_format": "multi_row_summary",
        }

    def generate_all_queries(self) -> pd.DataFrame:
        """
        Build the QA plan rows for each table with ONLY the requested fields:
          - query_id (INT-like, sequential per table group)
          - table_name (bare table name)
          - table_catalog
          - table_schema
          - check_type
          - query_text
          - expected_output_format
        """
        df = self.load_column_metadata()
        grouped = df.groupby(['table_catalog', 'table_schema', 'table_name'])['column_name'].apply(list)
        grouped = grouped.reset_index(name='columns')

        rows = []
        print("[INFO] Generating QA queries (count, nulls, hashcheck, duplicates)...")

        for _, row in grouped.iterrows():
            catalog = row['table_catalog']
            schema = row['table_schema']
            table = row['table_name']
            columns = row['columns'] or []

            # Per-table checks in a fixed order; assign integer query_id starting at 1
            per_table_checks = [
                self.build_count_query(catalog, schema, table),
                self.build_nulls_query(catalog, schema, table, columns),
                self.build_hashcheck_query(catalog, schema, table, columns),
                self.build_duplicates_query(catalog, schema, table, columns),
            ]

            for idx, chk in enumerate(per_table_checks, start=1):
                rows.append({
                    "query_id": idx,                 # sequential per (catalog,schema,table)
                    "table_name": table,             # bare table name only
                    "table_catalog": catalog,
                    "table_schema": schema,
                    "check_type": chk["check_type"],
                    "query_text": chk["query_text"],
                    "expected_output_format": chk["expected_output_format"],
                })

        out = pd.DataFrame(rows, columns=[
            "query_id",
            "table_name",
            "table_catalog",
            "table_schema",
            "check_type",
            "query_text",
            "expected_output_format",
        ])
        print(f"[INFO] Total QA queries generated: {len(out)}")
        return out

    def upload_to_bigquery(self, df: pd.DataFrame):
        """
        Load the generated plan to BigQuery (WRITE_TRUNCATE) with ONLY the requested columns.
        """
        table_ref = f"{self.project_id}.{self.output_table}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("query_id", "INT64"),
                bigquery.SchemaField("table_name", "STRING"),
                bigquery.SchemaField("table_catalog", "STRING"),
                bigquery.SchemaField("table_schema", "STRING"),
                bigquery.SchemaField("check_type", "STRING"),
                bigquery.SchemaField("query_text", "STRING"),
                bigquery.SchemaField("expected_output_format", "STRING"),
            ],
        )
        print(f"[INFO] Uploading {len(df)} QA queries to {table_ref} ...")
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"[INFO] Uploaded {len(df)} QA queries to {table_ref}")
