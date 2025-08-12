import pandas as pd
from google.cloud import bigquery


class QAQueryGeneratorFromColumns:
    def __init__(self, project_id: str, source_table: str, output_table: str):
        self.project_id = project_id
        self.source_table = source_table
        self.output_table = output_table
        self.client = bigquery.Client(project=project_id)

    def load_column_metadata(self) -> pd.DataFrame:
        query = f"""
            SELECT table_catalog, table_schema, table_name, column_name
            FROM `{self.project_id}.{self.source_table}`
        """
        return self.client.query(query).to_dataframe(create_bqstorage_client=False)

    def group_columns_by_table(self, df: pd.DataFrame) -> dict:
        grouped = df.groupby(['table_schema', 'table_name'])['column_name'].apply(list)
        return grouped.to_dict()

    @staticmethod
    def sanitize_column(col: str) -> str:
        return f"[{col}]"

    def build_count_query(self, schema: str, table: str, full_table: str) -> dict:
        return {
            "table_name": full_table,
            "check_type": "count_total_rows",
            "query_text": f"SELECT COUNT(*) AS total_rows FROM [{schema}].[{table}]",
            "expected_output_format": "single_row_scalar"
        }

    def build_nulls_query(self, schema: str, table: str, full_table: str, columns: list) -> dict:
        cols = [self.sanitize_column(c) for c in columns]
        null_checks = ",\n    ".join(
            f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS nulls_{c[1:-1]}" for c in cols
        )
        return {
            "table_name": full_table,
            "check_type": "nulls_per_column",
            "query_text": f"SELECT\n    {null_checks}\nFROM [{schema}].[{table}]",
            "expected_output_format": "single_row_scalar"
        }

    def build_hashcheck_query(self, schema: str, table: str, full_table: str, columns: list) -> dict:
        cols = [self.sanitize_column(c) for c in columns]
        concat_expr = ", ".join(f"CAST({c} AS NVARCHAR(MAX))" for c in cols)
        query = f"""
            SELECT *, 
            CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT_WS('|', {concat_expr})), 2) AS hashcheck 
            FROM [{schema}].[{table}]
        """.strip()
        return {
            "table_name": full_table,
            "check_type": "hashcheck",
            "query_text": query,
            "expected_output_format": "row_level"
        }

    def build_value_dist_query(self, schema: str, table: str, full_table: str, column: str) -> dict:
        col = self.sanitize_column(column)
        query = f"""
            SELECT {col}, COUNT(*) AS freq 
            FROM [{schema}].[{table}]
            GROUP BY {col}
            ORDER BY freq DESC
            OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY
        """.strip()
        return {
            "table_name": full_table,
            "check_type": f"value_dist_{column}",
            "query_text": query,
            "expected_output_format": "multi_row_summary"
        }

    def build_duplicates_query(self, schema: str, table: str, full_table: str, column: str) -> dict:
        col = self.sanitize_column(column)
        query = f"""
            SELECT {col}, COUNT(*) AS cnt 
            FROM [{schema}].[{table}]
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
        df = self.load_column_metadata()
        grouped = df.groupby(['table_catalog', 'table_schema', 'table_name'])['column_name'].apply(list)
        grouped = grouped.reset_index(name='columns')

        all_queries = []

        for _, row in grouped.iterrows():
            catalog = row['table_catalog']
            schema = row['table_schema']
            table = row['table_name']
            columns = row['columns']
            full_table = f"{schema}.{table}"

            all_queries.append({
                **self.build_count_query(schema, table, full_table),
                "table_catalog": catalog,
                "table_schema": schema,
                "table_name": table
            })
            all_queries.append({
                **self.build_nulls_query(schema, table, full_table, columns),
                "table_catalog": catalog,
                "table_schema": schema,
                "table_name": table
            })
            all_queries.append({
                **self.build_hashcheck_query(schema, table, full_table, columns),
                "table_catalog": catalog,
                "table_schema": schema,
                "table_name": table
            })
            if columns:
                all_queries.append({
                    **self.build_value_dist_query(schema, table, full_table, columns[0]),
                    "table_catalog": catalog,
                    "table_schema": schema,
                    "table_name": table
                })
                all_queries.append({
                    **self.build_duplicates_query(schema, table, full_table, columns[0]),
                    "table_catalog": catalog,
                    "table_schema": schema,
                    "table_name": table
                })

        return pd.DataFrame(all_queries)

    def upload_to_bigquery(self, df: pd.DataFrame):
        table_ref = f"{self.project_id}.{self.output_table}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Uploaded {len(df)} QA queries to {table_ref}")
