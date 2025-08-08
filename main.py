# main.py
import os
import uuid
import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import pandas as pd
import pyodbc
import gcsfs

from google.cloud import bigquery
from google.cloud import secretmanager


# -----------------------------
# Custom Pipeline Options
# -----------------------------
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Infra / project
        parser.add_argument('--gcp_project', required=True)

        # Extract params (SQL Server)
        parser.add_argument('--database', required=True)
        parser.add_argument('--schema', required=True)
        parser.add_argument('--table', required=True)

        parser.add_argument('--secret_id', default=None)          # opción 1: Secret Manager
        parser.add_argument('--connection_name', default=None)    # opción 2: alias 'fo' / 'bdw'

        parser.add_argument('--query', default=None)              # override opcional
        parser.add_argument('--primary_key', required=True)
        parser.add_argument('--type_of_extraction', required=True)  # full | change_tracking
        parser.add_argument('--reload_flag', required=True)          # true/false

        parser.add_argument('--batch_size', default='500000')     # str para venir del template
        parser.add_argument('--chunk_size', default='100000')

        parser.add_argument('--output_gcs_path', required=True)   # carpeta o path final

        # QA controls
        parser.add_argument('--qa_check_types', default='')       # CSV, vacío = todos
        parser.add_argument('--qa_limit', default='5')            # "5" por conveniencia
        parser.add_argument('--qa_results_table', required=True)  # <project>.<dataset>.<table>


# -----------------------------
# Conexiones SQL (si eliges alias simple)
# -----------------------------
CONNECTION_STRINGS = {
    "bdw": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=BrokerageWarehouse-UAT.RXO.COM\\BI;DATABASE=WarehouseBrokerage;Trusted_Connection=yes;",
    "fo":  "DRIVER={ODBC Driver 17 for SQL Server};SERVER=fbtdw2090.qaamer.qacorp.xpo.com;DATABASE=XPOMaster;Trusted_Connection=yes;"
}


# -----------------------------
# Helpers de conexión
# -----------------------------
def get_sql_config_from_secret(project_id: str, secret_id: str) -> dict:
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    payload = sm.access_secret_version(request={"name": name}).payload.data.decode("utf-8")
    return json.loads(payload)  # espera {driver, server, database, username, password}


def build_conn_str_from_secret(cfg: dict) -> str:
    return (
        f"DRIVER={{{cfg['driver']}}};"
        f"SERVER={cfg['server']},1433;"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};"
        f"PWD={cfg['password']};"
        f"Encrypt=yes;TrustServerCertificate=yes;Packet Size=512;"
    )


def resolve_sql_connection(options: CustomPipelineOptions) -> str:
    if options.secret_id:
        sec = get_sql_config_from_secret(options.gcp_project, options.secret_id)
        return build_conn_str_from_secret(sec)
    if options.connection_name:
        if options.connection_name not in CONNECTION_STRINGS:
            raise ValueError(f"Unknown connection_name: {options.connection_name}")
        return CONNECTION_STRINGS[options.connection_name]
    raise ValueError("Provide either --secret_id or --connection_name")


# -----------------------------
# Beam: Query a SQL Server
# -----------------------------
class QueryDatabase(beam.PTransform):
    def __init__(self, connection_string: str, query: str):
        self.connection_string = connection_string
        self.query = query

    def expand(self, pcoll):
        def fetch(_):
            conn = pyodbc.connect(self.connection_string)
            cur = conn.cursor()
            cur.execute(self.query)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.close()
            conn.close()
            return rows

        return pcoll | "FetchRows" >> beam.FlatMap(fetch)


# -----------------------------
# Beam: Write a single Parquet
# -----------------------------
class WriteSingleParquet(beam.DoFn):
    def __init__(self, output_path: str):
        self.output_path = output_path

    def process(self, elements):
        if not elements:
            return
        # Si output_path termina en '/', generamos nombre
        out = self.output_path
        if out.endswith('/'):
            ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            out = os.path.join(out, f"extract_{ts}_{uuid.uuid4().hex[:8]}.snappy.parquet")

        df = pd.DataFrame(elements)
        df.to_parquet(out, index=False, compression='snappy', storage_options={"token": "cloud"})
        yield out


# -----------------------------
# QA: Ejecutar QA en paralelo (DoFn)
# -----------------------------
class RunQAComparisonsFn(beam.DoFn):
    def __init__(self, project_id: str, qa_results_table: str):
        self.project_id = project_id
        self.qa_results_table = qa_results_table
        self.bq_client = None
        self.fs = None

    def setup(self):
        self.bq_client = bigquery.Client(project=self.project_id)
        self.fs = gcsfs.GCSFileSystem()

    def _run_bq_scalar(self, sql: str):
        df = self.bq_client.query(sql).result().to_dataframe(create_bqstorage_client=False)
        # Intentamos detectar un escalar (primer valor)
        if df.empty:
            return None, 0
        if df.shape == (1, 1):
            return df.iat[0, 0], 1
        return df.to_dict(orient='records'), len(df)

    def process(self, element):
        """
        element = {
            'parquet_path': 'gs://.../file.parquet',
            'qa': {
              'query_id': int,
              'check_type': str,
              'query_text': str,
              'table_catalog': str,
              'table_schema': str,
              'table_name': str
            }
        }
        """
        parquet_path = element['parquet_path']
        qa = element['qa']
        started_at = datetime.utcnow().isoformat()

        result_value, rowcount = None, 0
        status = "SUCCESS"
        err_msg = None

        try:
            # Ejecuta la QA en BigQuery tal cual viene (debe ser SQL estándar válido para BQ)
            result_value, rowcount = self._run_bq_scalar(qa['query_text'])
        except Exception as e:
            status = "ERROR"
            err_msg = str(e)

        yield {
            "run_ts": started_at,
            "query_id": qa.get("query_id"),
            "check_type": qa.get("check_type"),
            "table_catalog": qa.get("table_catalog"),
            "table_schema": qa.get("table_schema"),
            "table_name": qa.get("table_name"),
            "parquet_path": parquet_path,
            "rowcount": rowcount,
            "result_value": json.dumps(result_value, default=str) if not isinstance(result_value, (int, float, str, type(None))) else result_value,
            "status": status,
            "error": err_msg
        }


# -----------------------------
# Main pipeline
# -----------------------------
def run():
    pipeline_options = PipelineOptions(save_main_session=True)
    opts = pipeline_options.view_as(CustomPipelineOptions)

    # Construimos la query principal si no viene override
    if opts.query:
        main_query = opts.query
    else:
        main_query = f"SELECT * FROM [{opts.schema}].[{opts.table}]"

    # Resuelve conexión SQL Server
    conn_str = resolve_sql_connection(opts)

    # Arma WHERE para filtrar QA plan
    catalog = opts.database  # mapeamos database -> table_catalog en qa_query_plan
    qa_checks_csv = (opts.qa_check_types or "").strip()
    where_checks = ""
    if qa_checks_csv:
        # 'a,b,c' => IN ('a','b','c')
        checks = ",".join([f"'{c.strip()}'" for c in qa_checks_csv.split(",") if c.strip()])
        where_checks = f" AND check_type IN ({checks}) "

    qa_limit = int(opts.qa_limit or "5")

    qa_sql = f"""
        SELECT query_id, check_type, query_text, table_catalog, table_schema, table_name
        FROM `{opts.gcp_project}.dataops_admin.qa_query_plan`
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{opts.schema}'
          AND table_name = '{opts.table}'
          {where_checks}
        LIMIT {qa_limit}
    """

    # BQ schema para resultados
    qa_results_schema = {
        "fields": [
            {"name": "run_ts", "type": "TIMESTAMP"},
            {"name": "query_id", "type": "INT64"},
            {"name": "check_type", "type": "STRING"},
            {"name": "table_catalog", "type": "STRING"},
            {"name": "table_schema", "type": "STRING"},
            {"name": "table_name", "type": "STRING"},
            {"name": "parquet_path", "type": "STRING"},
            {"name": "rowcount", "type": "INT64"},
            {"name": "result_value", "type": "STRING"},
            {"name": "status", "type": "STRING"},
            {"name": "error", "type": "STRING"}
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        # 1) Extrae tabla y escribe Parquet (devuelve el path)
        parquet_path_pc = (
            p
            | "Start" >> beam.Create([None])
            | "QueryDatabase" >> QueryDatabase(connection_string=conn_str, query=main_query)
            | "BatchRows" >> beam.combiners.ToList()
            | "WriteParquet" >> beam.ParDo(WriteSingleParquet(opts.output_gcs_path))
        )

        # 2) Lee hasta 5 QA desde qa_query_plan (filtradas)
        qa_rows_pc = (
            p
            | "ReadQAPlan" >> beam.io.ReadFromBigQuery(
                query=qa_sql,
                use_standard_sql=True,
                project=opts.gcp_project
            )
        )

        # 3) Adjunta el parquet_path a cada QA row (side input)
        def attach_path(qa_row, p_path):
            return {"parquet_path": p_path, "qa": dict(qa_row)}

        qa_with_path = (
            qa_rows_pc
            | "AttachParquetPath" >> beam.Map(
                attach_path, p_path=beam.pvalue.AsSingleton(parquet_path_pc)
            )
        )

        # 4) Ejecuta las QA en paralelo y escribe resultados a BQ
        results = (
            qa_with_path
            | "RunQA" >> beam.ParDo(RunQAComparisonsFn(opts.gcp_project, opts.qa_results_table))
        )

        _ = (
            results
            | "WriteResultsBQ" >> beam.io.WriteToBigQuery(
                table=opts.qa_results_table,
                schema=qa_results_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()
