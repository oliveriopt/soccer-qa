import apache_beam as beam
from google.cloud import bigquery
import pandas as pd
import pyarrow.parquet as pq
import gcsfs
import logging
import os

class RunQAComparisonsFn(beam.DoFn):
    def __init__(self, gcp_project):
        self.gcp_project = gcp_project
        self.bq_client = None
        self.fs = gcsfs.GCSFileSystem()

    def setup(self):
        self.bq_client = bigquery.Client(project=self.gcp_project)

    def process(self, element):
        """
        element must be a dictionary containing:
        - query_id
        - query_text
        - table_catalog
        - table_schema
        - table_name
        - check_type
        - output_path
        """
        try:
            query_id = element["query_id"]
            query_text = element["query_text"]
            table_catalog = element["table_catalog"]
            table_schema = element["table_schema"]
            table_name = element["table_name"]
            check_type = element["check_type"]
            output_path = element["output_path"]

            # === Leer Parquet desde GCS ===
            logging.info(f"Reading Parquet file from: {output_path}")
            with self.fs.open(output_path, 'rb') as f:
                pq_table = pq.read_table(f)
            df_sql = pq_table.to_pandas()

            # === Ejecutar query en BigQuery ===
            bq_query = f"""
                {query_text}
            """
            df_bq = self.bq_client.query(bq_query).to_dataframe()

            # === Comparar resultados ===
            is_equal = df_sql.equals(df_bq)

            result = {
                "query_id": query_id,
                "table_catalog": table_catalog,
                "table_schema": table_schema,
                "table_name": table_name,
                "check_type": check_type,
                "status": "PASS" if is_equal else "FAIL",
                "row_count_sql": len(df_sql),
                "row_count_bq": len(df_bq),
                "output_path": output_path,
            }

            if not is_equal:
                logging.warning(f"QA CHECK FAILED for {query_id}: {output_path}")
            else:
                logging.info(f"QA CHECK PASSED for {query_id}")

            yield result

        except Exception as e:
            logging.error(f"Error in RunQAComparisonsFn: {e}")
            yield {
                "query_id": element.get("query_id", "unknown"),
                "status": "ERROR",
                "error_message": str(e),
                "output_path": element.get("output_path", "unknown")
            }
