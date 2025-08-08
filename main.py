# main.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery, BigQueryDisposition

import os
import json
import logging
import uuid
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq
import gcsfs
from google.cloud import bigquery
import pyodbc


# ---------------------------
# Pipeline Options (Flex params)
# ---------------------------
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Core / infra
        parser.add_argument('--gcp_project', required=True)
        parser.add_argument('--region', default=None)
        parser.add_argument('--staging_location', default=None)
        parser.add_argument('--temp_location', default=None)

        # Extract (table -> parquet)
        parser.add_argument('--connection_name', default=None)     # optional (simple demo)
        parser.add_argument('--secret_id', default=None)           # optional (si usas Secret Manager)
        parser.add_argument('--query', required=True)              # query principal a SQL Server
        parser.add_argument('--data_output_path', required=True)   # GCS parquet destino

        # QA plan filters
        parser.add_argument('--table_catalog', required=True)
        parser.add_argument('--table_schema', required=True)
        parser.add_argument('--table_name', required=True)
        parser.add_argument('--qa_check_types', default="")        # CSV (vacío -> todas)
        parser.add_argument('--qa_limit', default="5")             # máx 5

        # QA results sink
        parser.add_argument('--qa_results_table', required=True)

        # Worker tuning (opcionales)
        parser.add_argument('--service_account_email', default=None)
        parser.add_argument('--subnetwork', default=None)
        parser.add_argument('--disable_public_ips', default="true")
        parser.add_argument('--num_workers', default=None)
        parser.add_argument('--max_num_workers', default=None)
        parser.add_argument('--autoscaling_algorithm', default=None)
        parser.add_argument('--machine_type', default=None)


# ---------------------------
# Conexiones (simple demo con alias)
# ---------------------------
CONNECTION_STRINGS = {
    "fo":  "DRIVER={ODBC Driver 17 for SQL Server};SERVER=fbtdw2090.qaamer.qacorp.xpo.com;DATABASE=XPOMaster;Trusted_Connection=yes;",
    "bdw": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=BrokerageWarehouse-UAT.RXO.COM\\BI;DATABASE=WarehouseBrokerage;Trusted_Connection=yes;",
}


# ---------------------------
# Export: QueryDatabase -> WriteSingleParquet
# ---------------------------
class QueryDatabase(beam.PTransform):
    def __init__(self, connection_string, query):
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
        return pcoll | beam.FlatMap(fetch)


class WriteSingleParquet(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path
        self.fs = None

    def setup(self):
        self.fs = gcsfs.GCSFileSystem()

    def process(self, elements):
        if not elements:
            logging.info("No elements to write; skipping parquet.")
            return
        # respeta tu patrón de escribir 1 parquet
        path = self.output_path if not self.output_path.endswith('/') else os.path.join(
            self.output_path, f"part-{uuid.uuid4().hex[:8]}.snappy.parquet"
        )
        df = pd.DataFrame(elements)
        df.to_parquet(path, index=False, compression='snappy', storage_options={"token": "cloud"})
        logging.info(f"Wrote parquet to {path}")
        yield path  # downstream recibe el path


# ---------------------------
# QA DoFn
# ---------------------------
class RunQAComparisonsFn(beam.DoFn):
    """
    Recibe dicts con:
      {
        "parquet_path": <str>,
        "query_id": <str>,
        "check_type": <str>,
        "query_text": <str>    # en este ejemplo asumimos SQL de BigQuery para escalar (scalar)
      }
    """
    def __init__(self, gcp_project, qa_results_table, table_catalog, table_schema, table_name):
        self.gcp_project = gcp_project
        self.qa_results_table = qa_results_table
        self.table_catalog = table_catalog
        self.table_schema = table_schema
        self.table_name = table_name

        self.bq = None
        self.fs = None

    def setup(self):
        self.bq = bigquery.Client(project=self.gcp_project)
        self.fs = gcsfs.GCSFileSystem()

    def _read_parquet_row_count(self, parquet_path):
        with self.fs.open(parquet_path, 'rb') as f:
            tbl = pq.read_table(f)
            return tbl.num_rows

    def _run_bq_scalar(self, sql):
        job = self.bq.query(sql)
        rows = list(job.result())
        if not rows:
            return None
        # devuelve el primer valor de la primera fila
        d = dict(rows[0])
        return next(iter(d.values())) if d else None

    def process(self, element):
        parquet_path = element["parquet_path"]
        query_id = str(element["query_id"])
        check_type = element["check_type"]
        query_text = element["query_text"]

        run_ts = datetime.utcnow().isoformat()
        status = "PASS"
        details = {}

        try:
            if check_type == "count_total_rows":
                pq_count = self._read_parquet_row_count(parquet_path)
                bq_count = self._run_bq_scalar(query_text)

                if bq_count is None:
                    status = "ERROR"
                    details["error"] = "BQ query returned no rows"
                else:
                    status = "PASS" if int(bq_count) == int(pq_count) else "FAIL"
                    details = {"bq_count": int(bq_count), "parquet_count": int(pq_count), "diff": int(pq_count) - int(bq_count)}
            else:
                # Placeholder para otros check_types: ejecuta query de BQ y marca OK si no falla
                _ = self._run_bq_scalar(query_text)
                details = {"note": f"Executed check_type={check_type}. Extend comparison logic as needed."}

        except Exception as e:
            status = "ERROR"
            details = {"error": str(e)}

        yield {
            "run_id": str(uuid.uuid4()),
            "run_ts": run_ts,
            "query_id": query_id,
            "table_catalog": self.table_catalog,
            "table_schema": self.table_schema,
            "table_name": self.table_name,
            "check_type": check_type,
            "status": status,
            "details": json.dumps(details),
            "parquet_path": parquet_path,
        }


# ---------------------------
# Build BQ query to read up to 5 QA rows
# ---------------------------
def build_qa_plan_query(project_id, table_full, catalog, schema, table, check_types_csv, limit_n, effective_output_path=None):
    filter_types = ""
    if check_types_csv and check_types_csv.strip():
        items = [t.strip() for t in check_types_csv.split(",") if t.strip()]
        items = [t.replace("'", "\\'") for t in items]
        arr = ",".join([f"'{t}'" for t in items])
        filter_types = f"AND check_type IN ({arr})"

    limit_clause = f"LIMIT {int(limit_n) if limit_n else 5}"

    # Nota: si hoy tu qa_query_plan no guarda output_path STRING usable,
    # pasamos effective_output_path para que esté disponible si lo necesitas luego.
    sql = f"""
    SELECT
      CAST(query_id AS STRING) AS query_id,
      check_type,
      query_text,
      '{effective_output_path or ''}' AS effective_output_path,
      table_catalog,
      table_schema,
      table_name
    FROM `{project_id}.{table_full}`
    WHERE table_catalog = '{catalog}'
      AND table_schema  = '{schema}'
      AND table_name    = '{table}'
      {filter_types}
    ORDER BY query_id
    {limit_clause}
    """
    return sql


# ---------------------------
# Pipeline assembly
# ---------------------------
def run():
    pipeline_options = PipelineOptions(save_main_session=True)
    opts = pipeline_options.view_as(CustomPipelineOptions)

    gcp_project = opts.gcp_project
    qa_limit = int(opts.qa_limit) if opts.qa_limit else 5

    # Validaciones mínimas del export
    if not opts.connection_name and not opts.secret_id:
        raise ValueError("Provide either connection_name (demo) or implement secret_id resolution.")
    if not opts.query or not opts.data_output_path:
        raise ValueError("query and data_output_path are required (export step).")

    # Conexión simple por alias (si usas Secret Manager, resuélvelo aquí)
    conn_string = CONNECTION_STRINGS.get(opts.connection_name) if opts.connection_name else None
    if opts.secret_id and not conn_string:
        # Aquí podrías hacer lookup a Secret Manager y armar conn string.
        raise ValueError("secret_id resolution not implemented in this sample. Use connection_name or implement secret lookup.")

    # Query para leer hasta 5 QA del plan
    qa_plan_table = "dataops_admin.qa_query_plan"
    qa_sql = build_qa_plan_query(
        project_id=gcp_project,
        table_full=qa_plan_table,
        catalog=opts.table_catalog,
        schema=opts.table_schema,
        table=opts.table_name,
        check_types_csv=opts.qa_check_types or "",
        limit_n=qa_limit,
        effective_output_path=opts.data_output_path
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # 1) Export principal → Parquet (conservado como pediste)
        parquet_path_pc = (
            p
            | "Start" >> beam.Create([None])
            | "Query DB" >> QueryDatabase(connection_string=conn_string, query=opts.query)
            | "Batch All Rows" >> beam.combiners.ToList()          # ojo con tamaños grandes; ok para QA/UAT
            | "Write Parquet" >> beam.ParDo(WriteSingleParquet(opts.data_output_path))
        )
        # parquet_path_pc: PCollection[str] con UN solo elemento (el path)

        # 2) Lee hasta 5 QA del plan
        qa_rows = (
            p
            | "Read QA Plan" >> ReadFromBigQuery(
                query=qa_sql,
                use_standard_sql=True,
                project=gcp_project,
                gcs_location=opts.temp_location or None
            )
        )  # rows: query_id, check_type, query_text, ...

        # 3) Side input (lista de hasta 5) + expandir a 5 elementos por parquet
        qa_rows_side = qa_rows | "QA Rows ToList" >> beam.combiners.ToList()

        def attach_qa(parquet_path, qa_list):
            for r in qa_list:
                yield {
                    "parquet_path": parquet_path,
                    "query_id": r.get("query_id"),
                    "check_type": r.get("check_type"),
                    "query_text": r.get("query_text"),
                }

        qa_work = (
            parquet_path_pc
            | "Attach QA" >> beam.FlatMap(attach_qa, qa_list=beam.pvalue.AsList(qa_rows_side))
        )

        # 4) Ejecuta QA (paralelo) y 5) Persistir resultados en BQ
        qa_results = (
            qa_work
            | "Run QA" >> beam.ParDo(
                RunQAComparisonsFn(
                    gcp_project=gcp_project,
                    qa_results_table=opts.qa_results_table,
                    table_catalog=opts.table_catalog,
                    table_schema=opts.table_schema,
                    table_name=opts.table_name
                )
            )
        )

        qa_results | "Write QA Results" >> WriteToBigQuery(
            table=opts.qa_results_table,
            schema={
                "fields": [
                    {"name": "run_id", "type": "STRING"},
                    {"name": "run_ts", "type": "TIMESTAMP"},
                    {"name": "query_id", "type": "STRING"},
                    {"name": "table_catalog", "type": "STRING"},
                    {"name": "table_schema", "type": "STRING"},
                    {"name": "table_name", "type": "STRING"},
                    {"name": "check_type", "type": "STRING"},
                    {"name": "status", "type": "STRING"},
                    {"name": "details", "type": "STRING"},
                    {"name": "parquet_path", "type": "STRING"}
                ]
            },
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()