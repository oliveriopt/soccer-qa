import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyodbc
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import gcsfs
import json
import logging
import os
import uuid
import re
from google.cloud import bigquery
from google.cloud import secretmanager

# === typing ===
from typing import List, Dict, Optional


def get_sql_config(secret_id, project_id):
    print(f"[INFO] Accessing secret '{secret_id}' from default project context")
    logging.info(f"Accessing secret '{secret_id}' from default project context")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")
    print("[INFO] Secret retrieved successfully")
    logging.info("Secret retrieved successfully.")
    return json.loads(secret_payload)


def build_connection_string(config):
    print("[INFO] Building SQL Server connection string...")
    logging.info("Building SQL Server connection string...")
    return (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},1433;"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        "Packet Size=512;"
    )


class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--gcp_project', required=True)
        parser.add_argument('--database', required=True)
        parser.add_argument('--schema', required=True)
        parser.add_argument('--table', required=True)
        parser.add_argument('--reload_flag', type=str, default="false")
        parser.add_argument('--type_of_extraction', required=True)
        parser.add_argument('--batch_size', type=int, default=50000)
        parser.add_argument('--output_gcs_path', required=True)
        parser.add_argument('--query', type=str, default='')
        parser.add_argument('--secret_id', required=True)
        parser.add_argument('--primary_key', required=True)
        parser.add_argument('--chunk_size', type=int, default=100000)

        # === QA integration: new args ===
        parser.add_argument(
            '--qa_check_types',
            type=str,
            default="",
            help="List of QA check types separated by '|' (e.g., 'count|nulls|hashcheck')."
        )
        parser.add_argument('--qa_plan_dataset', type=str, default="dataops_admin",
                            help="BigQuery dataset for qa_query_plan.")
        parser.add_argument('--qa_plan_table', type=str, default="qa_query_plan",
                            help="Table name of the QA plan in BigQuery.")


def camel_to_snake_case(name: str) -> str:
    """
    Converts CamelCase or PascalCase to snake_case.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.replace("__", "_").lower()


def get_casted_column_expressions(schema, table, conn):
    """Returns a list of casted column expressions and column names."""
    unsupported_types = {"datetimeoffset", "sql_variant", "geometry", "hierarchyid", "geography", "timestamp"}
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'
    """)
    expressions = []
    column_names = []
    for col, dtype in cursor.fetchall():
        column_names.append(col)
        if dtype.lower() in unsupported_types:
            expressions.append(f"CAST([{col}] AS VARCHAR(100)) AS [{col}]")
        else:
            expressions.append(f"[{col}] AS [{col}]")
    return expressions, column_names


def get_max_sys_change_version_bq(project_id: str, dataset: str, table: str) -> int:
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"
    try:
        query = f"SELECT MAX(sys_change_version) AS max_version FROM `{table_ref}`"
        result = client.query(query).result()
        row_list = list(result)
        row = row_list[0] if row_list else None
        return int(row.max_version) if row and row.max_version is not None else 0
    except Exception as e:
        print(f"[WARN] Could not query sys_change_version from {table_ref}: {e}")
        logging.warning(f"Could not query sys_change_version from {table_ref}: {e}")
        return 0  # default to 0 if column/table doesn't exist


def generate_batch_queries(database, schema, table, batch_size, connection_string, reload_flag, type_of_extraction, primary_key, min_version_bq):
    print(f"[INFO] Generating batch queries for table: {schema}.{table}")
    logging.info(f"Generating batch queries for table: {schema}.{table}")

    def cast_expr(col, prefix=""):
        col_ref = f"{prefix}[{col}]" if prefix else f"[{col}]"
        return f"CAST({col_ref} AS NVARCHAR(MAX)) AS [{col}]"

    queries = []

    conn = None
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Total rows in base table
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        total_rows = cursor.fetchone()[0]
        print(f"[INFO] Total rows in table: {total_rows}")
        logging.info(f"Total rows in table: {total_rows}")

        # Total rows in change tracking
        cursor.execute(f"SELECT COUNT(*) FROM CHANGETABLE(CHANGES [{schema}].[{table}], 0) AS CT")
        total_rows_ct = cursor.fetchone()[0]
        print(f"[INFO] Total rows in change tracking: {total_rows_ct}")
        logging.info(f"Total rows in change tracking: {total_rows_ct}")

        # Max SYS_CHANGE_VERSION
        cursor.execute(f"SELECT MAX(SYS_CHANGE_VERSION) FROM CHANGETABLE(CHANGES [{schema}].[{table}], 0) AS CT")
        max_ct_version = cursor.fetchone()[0] or 0
        print(f"[INFO] Max SYS_CHANGE_VERSION: {max_ct_version}")
        logging.info(f"Max SYS_CHANGE_VERSION: {max_ct_version}")
        bq_dataset = "sqlserver_to_bq_silver"
        
        # Column metadata
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'
        """)
        columns = [row.COLUMN_NAME for row in cursor.fetchall()]

        if reload_flag or type_of_extraction.lower() == "full":
            select_casts = ",\n    ".join([cast_expr(col) for col in columns])
            for start in range(1, total_rows + 1, batch_size):
                end = start + batch_size - 1
                query = f"""
                    SELECT * FROM (
                        SELECT 
                            {select_casts},
                            cast({max_ct_version} as nvarchar(max)) AS sys_change_version,
                            cast('I' as nvarchar(max)) AS sys_change_operation,
                            cast(GETUTCDATE() as nvarchar(max)) AS ingestion_datetime_utc,
                            cast(ROW_NUMBER() OVER (ORDER BY [{primary_key}]) as nvarchar(max)) AS rn
                        FROM [{schema}].[{table}]
                    ) AS numbered
                    WHERE rn BETWEEN {start} AND {end}
                """
                logging.info(f"Generated FULL query for rows {start}-{end}")
                queries.append(query.strip())

        elif type_of_extraction.lower() == "change_tracking":
            select_casts_ct = ",\n    ".join([cast_expr(col, prefix="base_sql_table.") for col in columns])
            for start in range(1, total_rows_ct + 1, batch_size):
                end = start + batch_size - 1
                query = f"""
                    SELECT * FROM (
                        SELECT 
                            {select_casts_ct},
                            cast(c.[SYS_CHANGE_VERSION] as nvarchar(max)) as sys_change_version,
                            cast(c.[SYS_CHANGE_OPERATION] as nvarchar(max)) as sys_change_operation,
                            cast(GETUTCDATE() as nvarchar(max)) AS ingestion_datetime_utc,
                            cast(ROW_NUMBER() OVER (ORDER BY base_sql_table.[{primary_key}]) as nvarchar(max)) AS rn
                        FROM CHANGETABLE(CHANGES [{schema}].[{table}], {min_version_bq}) AS c
                        LEFT JOIN [{schema}].[{table}] AS base_sql_table
                        ON c.[{primary_key}] = base_sql_table.[{primary_key}]
                    ) AS numbered
                    WHERE rn BETWEEN {start} AND {end}
                """
                logging.info(f"Generated CT query for rows {start}-{end}")
                queries.append(query.strip())
        else:
            raise ValueError("Unsupported type_of_extraction. Use 'full' or 'change_tracking'.")

    finally:
        if conn:
            conn.close()

    return queries


class StreamedChunkedReader(beam.DoFn):
    """Reads rows in chunks from SQL Server for the generated batched queries."""
    def __init__(self, connection_string, chunk_size):
        self.connection_string = connection_string
        self.chunk_size = chunk_size
        self.conn = None

    def start_bundle(self):
        self.conn = pyodbc.connect(self.connection_string)

    def process(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        chunk = []
        for row in cursor:
            record = {}
            for col_name, value in zip(columns, row):
                snake_col = camel_to_snake_case(col_name)
                if isinstance(value, (bytes, bytearray)):
                    record[snake_col] = value.decode(errors="ignore")
                elif isinstance(value, datetime):
                    record[snake_col] = value.isoformat()
                else:
                    record[snake_col] = value
            chunk.append(record)
            if len(chunk) >= self.chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
        cursor.close()

    def finish_bundle(self):
        if self.conn:
            self.conn.close()


class WriteParquetPerBatch(beam.DoFn):
    """Writes each chunk of rows to a Parquet file in GCS."""
    def __init__(self, output_gcs_path):
        self.output_gcs_path = output_gcs_path
        self.fs = gcsfs.GCSFileSystem()

    def process(self, elements):
        if not elements:
            return

        batch_id = uuid.uuid4().hex[:8]
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(self.output_gcs_path, f"{timestamp}_part-{batch_id}.parquet")

        try:
            table = pa.Table.from_pylist(elements)
            with self.fs.open(output_file, 'wb') as f:
                pq.write_table(table, f, compression='snappy')
            print(f"[INFO] Wrote {len(elements)} rows to {output_file}")
            logging.info(f"Wrote {len(elements)} rows to {output_file}")
            yield output_file
        except Exception as e:
            print(f"[ERROR] Failed to write Parquet: {e}")
            logging.error(f"Failed to write Parquet: {e}")
            raise


# === QA integration: helpers to parse list and read plan from BQ ===
def _parse_qa_check_types(pipe_text: str) -> List[str]:
    if not pipe_text:
        return []
    parts = [p.strip().lower() for p in pipe_text.split('|')]
    return [p for p in parts if p]


def read_qa_query_plan(
    gcp_project: str,
    qa_plan_dataset: str,
    qa_plan_table: str,
    database: str,
    schema: str,
    table: str,
    check_types: List[str],
) -> List[Dict]:
    """
    Reads <project>.<qa_plan_dataset>.<qa_plan_table> filtering by:
      - table_catalog = database
      - table_schema  = schema
      - table_name    = table
      - check_type IN UNNEST(check_types)
    Returns a list of dicts: query_id, table_name, check_type, query_text, expected_output_format, table_catalog, table_schema
    """
    if not check_types:
        print("[WARN] QA | 'qa_check_types' empty. Returning empty list.")
        logging.warning("QA | 'qa_check_types' empty. Returning empty list.")
        return []

    full_table = f"`{gcp_project}.{qa_plan_dataset}.{qa_plan_table}`"
    client = bigquery.Client(project=gcp_project)

    # NOTE: using literal interpolation to keep changes minimal (your code path)
    database_l = f"'{database}'"
    schema_l = f"'{schema}'"
    table_l = f"'{table}'"
    check_types_lit = f"{check_types}"

    print(f"[INFO] QA | Reading QA plan from {full_table} (database={database_l}, schema={schema_l}, table={table_l}, check_types={check_types_lit})")
    logging.info(
        "QA | Reading QA plan from %s (database=%s, schema=%s, table=%s, check_types=%s)",
        full_table, database_l, schema_l, table_l, check_types_lit
    )

    query = f"""
    SELECT
      query_id,
      table_name,
      check_type,
      query_text,
      expected_output_format,
      table_catalog,
      table_schema
    FROM {full_table}
    WHERE table_catalog = {database_l}
      AND table_schema  = {schema_l}
      AND table_name    = {table_l}
      AND check_type IN UNNEST({check_types_lit})
    ORDER BY query_id
    """

    print("[DEBUG] QA | Query to BQ:\n" + query)
    rows = list(client.query(query).result())

    result: List[Dict] = []
    for r in rows:
        result.append({
            "query_id": r.get("query_id"),
            "table_name": r.get("table_name"),
            "check_type": r.get("check_type"),
            "query_text": r.get("query_text"),
            "expected_output_format": r.get("expected_output_format"),
            "table_catalog": r.get("table_catalog"),
            "table_schema": r.get("table_schema"),
        })

    print(f"[INFO] QA | Rows fetched from qa_query_plan: {len(result)}")
    logging.info("QA | Rows fetched from qa_query_plan: %d", len(result))
    return result


# === NEW: DoFn to execute QA queries in parallel against SQL Server ===
class RunQAQuery(beam.DoFn):
    """
    Executes a QA query_text on SQL Server and returns a small result dict.
    Minimally invasive: one connection per bundle, robust error capture, scalar-first.
    """
    def __init__(self, connection_string: str, timeout_sec: int = 600):
        self.connection_string = connection_string
        self.timeout_sec = timeout_sec
        self.conn = None

    def start_bundle(self):
        print("[INFO] QA | Opening SQL Server connection for QA bundle")
        self.conn = pyodbc.connect(self.connection_string, timeout=self.timeout_sec)
        self.conn.autocommit = True

    def process(self, qa_row: Dict):
        out = {
            "query_id": qa_row.get("query_id"),
            "table_name": qa_row.get("table_name"),
            "check_type": qa_row.get("check_type"),
            "status": "OK",
            "value": None,
            "error": None,
            "executed_at_utc": datetime.utcnow().isoformat()
        }
        try:
            cur = self.conn.cursor()
            cur.execute(qa_row["query_text"])
            row = cur.fetchone()
            # Prefer scalar if available; otherwise sample small tabular output
            if row is not None and (cur.description is None or len(cur.description) == 1):
                out["value"] = row[0]
            else:
                cols = [d[0] for d in cur.description] if cur.description else []
                rows = []
                if cols:
                    for i, r in enumerate(cur.fetchmany(1000)):  # cap to avoid huge payloads
                        rows.append(dict(zip(cols, r)))
                        if i >= 999:
                            break
                out["value"] = rows
            cur.close()
        except Exception as e:
            out["status"] = "ERROR"
            out["error"] = str(e)
        yield out

    def finish_bundle(self):
        if self.conn:
            print("[INFO] QA | Closing SQL Server connection for QA bundle")
            self.conn.close()


def run_pipeline():
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project='rxo-dataeng-datalake-np',
        region='us-central1',
        temp_location='gs://rxo-dataeng-datalake-np-dataflow/temp',
        staging_location='gs://rxo-dataeng-datalake-np-dataflow/staging',
        save_main_session=True,
        service_account_email='ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com',
        subnetwork='https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1',
        use_public_ips=False,
        worker_harness_container_image='us-central1-docker.pkg.dev/rxo-dataeng-datalake-np/dataflow-flex-template/sql-pqt-to-gcs:latest'
    )
    custom_options = pipeline_options.view_as(CustomPipelineOptions)
    print(custom_options)

    # Build SQL Server connection string from Secret Manager
    config = get_sql_config(custom_options.secret_id, custom_options.gcp_project)
    connection_string = build_connection_string(config)

    # BQ max change version (used by extraction branch)
    bq_table = f"{custom_options.database.lower()}_{custom_options.schema.lower()}_{custom_options.table.lower()}"
    bq_dataset = "sqlserver_to_bq_silver"
    max_version_bq = get_max_sys_change_version_bq(custom_options.gcp_project, bq_dataset, bq_table)
    print(f"[INFO] Max sys_change_version from BigQuery: {max_version_bq}")
    logging.info(f"Max sys_change_version from BigQuery: {max_version_bq}")

    # === QA plan: read queries to execute against SQL Server ===
    qa_check_types = _parse_qa_check_types(getattr(custom_options, "qa_check_types", ""))
    print(f"[INFO] QA check types: {qa_check_types} ({type(qa_check_types)})")

    qa_plan_dataset = getattr(custom_options, "qa_plan_dataset", "dataops_admin")
    qa_plan_table = getattr(custom_options, "qa_plan_table", "qa_query_plan")

    qa_rows = read_qa_query_plan(
        gcp_project=custom_options.gcp_project,
        qa_plan_dataset=qa_plan_dataset,
        qa_plan_table=qa_plan_table,
        database=custom_options.database,
        schema=custom_options.schema,
        table=custom_options.table,
        check_types=qa_check_types
    )
    print(f"[INFO] QA | Total QA definitions found: {len(qa_rows)}")
    logging.info(f"QA | Total definitions found: {len(qa_rows)}")

    # === Extraction queries (original branch) ===
    queries = generate_batch_queries(
        database=custom_options.database,
        schema=custom_options.schema,
        table=custom_options.table,
        batch_size=custom_options.batch_size,
        connection_string=connection_string,
        reload_flag=custom_options.reload_flag.strip().lower() in ("true", "1", "yes"),
        type_of_extraction=custom_options.type_of_extraction,
        primary_key=custom_options.primary_key,
        min_version_bq=max_version_bq
    )

    print(f"[INFO] Total extraction batch queries: {len(queries)}")

    # Build a timestamped prefix for QA results on GCS
    qa_prefix = os.path.join(
        custom_options.output_gcs_path,
        "qa_results",
        datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Branch 1: Original extraction pipeline (unchanged)
        (
            p
            | "Create Queries" >> beam.Create(queries)
            | "Read & Chunk SQL" >> beam.ParDo(StreamedChunkedReader(connection_string, custom_options.chunk_size))
            | "Write Parquet" >> beam.ParDo(WriteParquetPerBatch(custom_options.output_gcs_path))
        )

        # Branch 2: QA execution in parallel (NEW)
        (
            p
            | "Create QA Rows" >> beam.Create(qa_rows)
            | "Run QA Queries (SQL Server)" >> beam.ParDo(RunQAQuery(connection_string))
            | "QA to JSON" >> beam.Map(json.dumps)
            | "Write QA JSONL" >> beam.io.WriteToText(qa_prefix, file_name_suffix=".jsonl", num_shards=1)
        )

    print("[INFO] Pipeline finished.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run_pipeline()
