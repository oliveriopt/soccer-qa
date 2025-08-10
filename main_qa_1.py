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
from google.cloud import bigquery
from google.cloud import secretmanager

# === QA integration: extra imports (typing) ===
from typing import List, Dict, Optional

def get_sql_config(secret_id, project_id):
    logging.info(f"Accessing secret '{secret_id}' from default project context")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")
    logging.info("Secret retrieved successfully.")
    return json.loads(secret_payload)


def build_connection_string(config):
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
        parser.add_argument('--query',type=str,default='')
        parser.add_argument('--secret_id', required=True)
        parser.add_argument('--primary_key', required=True)
        parser.add_argument('--chunk_size', type=int, default=100000)

        # === QA integration: nuevos argumentos ===
        parser.add_argument('--qa_check_types', type=str, default="",
                            help="Lista CSV de tipos de check (p.ej. 'count,nulls,hashcheck').")
        parser.add_argument('--qa_plan_dataset', type=str, default="dataops_admin",
                            help="Dataset de BigQuery para qa_query_plan.")
        parser.add_argument('--qa_plan_table', type=str, default="qa_query_plan",
                            help="Tabla con el plan de QA.")

def camel_to_snake_case(name: str) -> str:
    """
    Converts CamelCase or PascalCase to snake_case.
    
    Examples:
    - 'SalespersonID' -> 'salesperson_id'
    - 'UserUUID' -> 'user_uuid'
    - 'APIResponseCode' -> 'api_response_code'
    - 'createdAt' -> 'created_at'
    """
    # Handle acronyms and capital letters followed by lowercase
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    # Handle lowercase-to-uppercase transitions (e.g., 'ID', 'UUID')
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    # Convert to lowercase and normalize multiple underscores
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
        query = f"""
            query = f"SELECT MAX(sys_change_version) as max_version FROM `{table_ref}`
        """
        result = client.query(query).result()
        row = list(result)[0]
        return int(row.max_version) if row and row.max_version is not None else 0
    except Exception as e:
        logging.warning(f"Could not query sys_change_version from {table_ref}: {e}")
        return 0  # default to 0 if column/table doesn't exist

def generate_batch_queries(database,schema, table, batch_size, connection_string, reload_flag, type_of_extraction, primary_key,min_version_bq):
    logging.info(f"Generating batch queries for table: {schema}.{table}")

    def cast_expr(col, prefix=""):
        col_ref = f"{prefix}[{col}]" if prefix else f"[{col}]"
        return f"CAST({col_ref} AS NVARCHAR(MAX)) AS [{col}]"

    queries = []

    conn = None
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Get total rows
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        total_rows = cursor.fetchone()[0]
        logging.info(f"Total rows in table: {total_rows}")

        # Get total change tracking rows
        cursor.execute(f"SELECT COUNT(*) FROM CHANGETABLE(CHANGES [{schema}].[{table}], 0) AS CT")
        total_rows_ct = cursor.fetchone()[0]
        logging.info(f"Total rows in change tracking: {total_rows_ct}")

        # Get max SYS_CHANGE_VERSION
        cursor.execute(f"SELECT MAX(SYS_CHANGE_VERSION) FROM CHANGETABLE(CHANGES [{schema}].[{table}], 0) AS CT")
        max_ct_version = cursor.fetchone()[0] or 0
        logging.info(f"Max SYS_CHANGE_VERSION: {max_ct_version}")
        bq_dataset = "sqlserver_to_bq_silver"
        
        # Get column metadata
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
                logging.info(f"Generated FULL query for rows {start}-{end}:\n{query}")
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
                logging.info(f"Generated CT query for rows {start}-{end}:\n{query}")
                queries.append(query.strip())
        else:
            raise ValueError("Unsupported type_of_extraction. Use 'full' or 'change_tracking'.")

    finally:
        if conn:
            conn.close()

    return queries



class StreamedChunkedReader(beam.DoFn):
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
            logging.info(f"Wrote {len(elements)} rows to {output_file}")
            yield output_file
        except Exception as e:
            logging.error(f"Failed to write Parquet: {e}")
            raise

# === QA integration: helpers para lista y lectura desde BQ ===
def _parse_qa_check_types(csv_text: str) -> List[str]:
    if not csv_text:
        return []
    parts = [p.strip().lower() for p in csv_text.split(',')]
    return [p for p in parts if p]

def read_qa_query_plan(
    gcp_project: str,
    qa_plan_dataset: str,
    qa_plan_table: str,
    database: str,
    schema: str,
    check_types: List[str],
) -> List[Dict]:
    """
    Lee <project>.<qa_plan_dataset>.<qa_plan_table> filtrando por:
      - table_catalog = database
      - table_schema  = schema
      - LOWER(check_type) IN check_types
    Devuelve lista de dicts con: query_id, table_name, check_type, query_text, expected_ouput_format, table_catalog, table_schema
    """
    if not check_types:
        logging.warning("QA | 'qa_check_types' vacío. Retornando lista vacía.")
        return []

    full_table = f"`{gcp_project}.{qa_plan_dataset}.{qa_plan_table}`"
    client = bigquery.Client(project=gcp_project)

    logging.info(
        "QA | Leyendo plan de QA desde %s (database=%s, schema=%s, check_types=%s)",
        full_table, database, schema, check_types
    )

    query = f"""
    SELECT
      query_id,
      table_name,
      check_type,
      query_text,
      expected_ouput_format,
      table_catalog,
      table_schema
    FROM {full_table}
    WHERE table_catalog = @database
      AND table_schema  = @schema
      AND LOWER(check_type) IN UNNEST(@check_types)
    ORDER BY query_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("database", "STRING", database),
            bigquery.ScalarQueryParameter("schema", "STRING", schema),
            bigquery.ArrayQueryParameter("check_types", "STRING", check_types),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())

    result: List[Dict] = []
    for r in rows:
        result.append({
            "query_id": r.get("query_id"),
            "table_name": r.get("table_name"),
            "check_type": r.get("check_type"),
            "query_text": r.get("query_text"),
            "expected_ouput_format": r.get("expected_ouput_format"),
            "table_catalog": r.get("table_catalog"),
            "table_schema": r.get("table_schema"),
        })

    logging.info("QA | Filas recuperadas desde qa_query_plan: %d", len(result))
    if result:
        sample = result[0]
        logging.info(
            "QA | Ejemplo -> query_id=%s, table=%s, check=%s, format=%s",
            sample.get("query_id"), sample.get("table_name"),
            sample.get("check_type"), sample.get("expected_ouput_format")
        )
    else:
        logging.warning("QA | No se encontraron filas con los filtros dados.")
    return result


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
    config = get_sql_config(custom_options.secret_id, custom_options.gcp_project)
    connection_string = build_connection_string(config)
    bq_table = f"{custom_options.database.lower()}_{custom_options.schema.lower()}_{custom_options.table.lower()}"
    bq_dataset ="sqlserver_to_bq_silver"
    max_version_bq = get_max_sys_change_version_bq(custom_options.gcp_project, bq_dataset, bq_table)
    logging.info(f"Max sys_change_version from BigQuery: {max_version_bq}")

    # === QA integration: leer qa_query_plan (solo logging, sin modificar el flujo) ===
    qa_check_types = _parse_qa_check_types(getattr(custom_options, "qa_check_types", ""))
    qa_plan_dataset = getattr(custom_options, "qa_plan_dataset", "dataops_admin")
    qa_plan_table   = getattr(custom_options, "qa_plan_table", "qa_query_plan")

    qa_rows = read_qa_query_plan(
        gcp_project=custom_options.gcp_project,
        qa_plan_dataset=qa_plan_dataset,
        qa_plan_table=qa_plan_table,
        database=custom_options.database,
        schema=custom_options.schema,
        check_types=qa_check_types
    )
    logging.info(f"QA | Total definiciones encontradas: {len(qa_rows)}")

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

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Create Queries" >> beam.Create(queries)
            | "Read & Chunk SQL" >> beam.ParDo(StreamedChunkedReader(connection_string, custom_options.chunk_size))
            | "Write Parquet" >> beam.ParDo(WriteParquetPerBatch(custom_options.output_gcs_path))
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run_pipeline()



{
  "name": "sql-pqt-to-gcs",
  "description": "Extrae datos de SQL Server (full o change_tracking) en lotes y los guarda en Parquet en GCS. Integra lectura de definiciones de QA desde qa_query_plan en BigQuery.",
  "parameters": [
    {"name": "gcp_project", "label": "GCP Project", "helpText": "Proyecto donde están los secretos y BigQuery.", "isOptional": false},
    {"name": "database", "label": "Database (SQL Server)", "isOptional": false},
    {"name": "schema", "label": "Schema (SQL Server)", "isOptional": false},
    {"name": "table", "label": "Table (SQL Server)", "isOptional": false},
    {"name": "reload_flag", "label": "Reload Flag (true/false)", "defaultValue": "false", "isOptional": true},
    {"name": "type_of_extraction", "label": "Extraction Type", "helpText": "full | change_tracking", "isOptional": false},
    {"name": "batch_size", "label": "Batch Size", "defaultValue": "50000", "isOptional": true},
    {"name": "output_gcs_path", "label": "Output GCS Path", "isOptional": false},
    {"name": "query", "label": "Custom SQL Query (opcional)", "defaultValue": "", "isOptional": true},
    {"name": "secret_id", "label": "Secret ID", "helpText": "Nombre del secreto en Secret Manager con credenciales de SQL Server.", "isOptional": false},
    {"name": "primary_key", "label": "Primary Key", "isOptional": false},
    {"name": "chunk_size", "label": "Chunk Size (SQL fetch)", "defaultValue": "100000", "isOptional": true},

    {"name": "qa_check_types", "label": "QA Check Types CSV", "helpText": "Lista CSV de tipos de check de QA (p.ej. 'count,nulls,hashcheck').", "defaultValue": "", "isOptional": true},
    {"name": "qa_plan_dataset", "label": "QA Plan Dataset", "defaultValue": "dataops_admin", "isOptional": true},
    {"name": "qa_plan_table", "label": "QA Plan Table", "defaultValue": "qa_query_plan", "isOptional": true}
  ]
}


gcloud dataflow flex-template run "sqlpqt-$(date +%Y%m%d-%H%M%S)" \
  --project=rxo-dataeng-datalake-np \
  --region=us-central1 \
  --template-file-gcs-location=gs://rxo-dataeng-datalake-np-dataflow/templates/sql-pqt-to-gcs.json \
  --parameters=^:^\
gcp_project=rxo-dataeng-datalake-np:\
database=MyDB:\
schema=dbo:\
table=MyTable:\
reload_flag=false:\
type_of_extraction=change_tracking:\
batch_size=50000:\
output_gcs_path=gs://rxo-dataeng-datalake-np-dataflow/output/MyDB/dbo/MyTable:\
query=:\
secret_id=sqlserver_conn:\
primary_key=Id:\
chunk_size=100000:\
qa_check_types=count,nulls,hashcheck:\
qa_plan_dataset=dataops_admin:\
qa_plan_table=qa_query_plan
