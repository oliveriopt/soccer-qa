¡Listo! Aquí tienes el archivo principal completo con la lógica corregida para construir el expected_map desde los resultados SQL (incluyendo el desglosado de nulls_per_column) y compararlo contra lo observado en los Parquet generados.
Todo está comentado en inglés y los print() también.

Puntos clave de esta versión:
	•	Ejecuta extracción → escribe Parquet → calcula métricas observadas por shard → combina a un solo mapa observed_map.
	•	Ejecuta QA SQL en paralelo → combina todos los resultados a un solo expected_map (extrae el escalar de count_total_rows y “explota” las columnas nulls_<Col> a claves nulls_per_column|<col_en_snake_case>).
	•	Compara observed_map vs expected_map y escribe alerts JSONL (OK, MISMATCH, MISSING_SQL).
	•	Se cuida que los tipos encajen con CombineFn (ya no verás el TypeCheckError).

⸻


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
from collections import defaultdict
from typing import List, Dict, Optional

from google.cloud import bigquery
from google.cloud import secretmanager


# =============================
# Helpers: logging + naming
# =============================

def camel_to_snake_case(name: str) -> str:
    """
    Converts CamelCase or PascalCase to snake_case (lowercase).
    """
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.replace('__', '_').lower()


def _safe_int(x):
    """
    Convert value to int safely; falls back to 0 on failure.
    """
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return 0


# =============================
# Secrets / Connection
# =============================

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


# =============================
# Beam options
# =============================

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

        # QA args
        parser.add_argument(
            '--qa_check_types',
            type=str,
            default="",
            help="List of QA check types separated by '|' (e.g., 'count_total_rows|nulls_per_column')."
        )
        parser.add_argument('--qa_plan_dataset', type=str, default="dataops_admin",
                            help="BigQuery dataset for qa_query_plan.")
        parser.add_argument('--qa_plan_table', type=str, default="qa_query_plan",
                            help="Table name of the QA plan in BigQuery.")


# =============================
# SQL Server read / batching
# =============================

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
        return 0


def generate_batch_queries(database, schema, table, batch_size, connection_string,
                           reload_flag, type_of_extraction, primary_key, min_version_bq):
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

        # Base counts
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        total_rows = cursor.fetchone()[0]
        print(f"[INFO] Total rows in table: {total_rows}")

        cursor.execute(f"SELECT COUNT(*) FROM CHANGETABLE(CHANGES [{schema}].[{table}], 0) AS CT")
        total_rows_ct = cursor.fetchone()[0]
        print(f"[INFO] Total rows in change tracking: {total_rows_ct}")

        cursor.execute(f"SELECT MAX(SYS_CHANGE_VERSION) FROM CHANGETABLE(CHANGES [{schema}].[{table}], 0) AS CT")
        max_ct_version = cursor.fetchone()[0] or 0
        print(f"[INFO] Max SYS_CHANGE_VERSION: {max_ct_version}")

        # Columns
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
                            CAST({max_ct_version} AS NVARCHAR(MAX)) AS sys_change_version,
                            CAST('I' AS NVARCHAR(MAX)) AS sys_change_operation,
                            CAST(GETUTCDATE() AS NVARCHAR(MAX)) AS ingestion_datetime_utc,
                            CAST(ROW_NUMBER() OVER (ORDER BY [{primary_key}]) AS NVARCHAR(MAX)) AS rn
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
                            CAST(c.[SYS_CHANGE_VERSION] AS NVARCHAR(MAX)) AS sys_change_version,
                            CAST(c.[SYS_CHANGE_OPERATION] AS NVARCHAR(MAX)) AS sys_change_operation,
                            CAST(GETUTCDATE() AS NVARCHAR(MAX)) AS ingestion_datetime_utc,
                            CAST(ROW_NUMBER() OVER (ORDER BY base_sql_table.[{primary_key}]) AS NVARCHAR(MAX)) AS rn
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
    """
    Reads rows in chunks from SQL Server for the generated batched queries.
    Emits a list[dict] per chunk (to be written into a Parquet file).
    """
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
    """
    Writes each chunk of rows to a Parquet file in GCS.
    Emits the GCS path of the created Parquet file.
    """
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


# =============================
# QA Plan read from BigQuery
# =============================

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
    Returns list of dicts: query_id, table_name, check_type, query_text, expected_output_format, table_catalog, table_schema
    """
    if not check_types:
        print("[WARN] QA | 'qa_check_types' empty. Returning empty list.")
        logging.warning("QA | 'qa_check_types' empty. Returning empty list.")
        return []

    full_table = f"`{gcp_project}.{qa_plan_dataset}.{qa_plan_table}`"
    client = bigquery.Client(project=gcp_project)

    database_l = f"'{database}'"
    schema_l = f"'{schema}'"
    table_l = f"'{table}'"
    check_types_lit = f"{check_types}"

    print(f"[INFO] QA | Reading QA plan from {full_table} (database={database_l}, schema={schema_l}, table={table_l}, check_types={check_types_lit})")
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

    rows = list(client.query(query).result())

    out: List[Dict] = []
    for r in rows:
        out.append({
            "query_id": r.get("query_id"),
            "table_name": r.get("table_name"),
            "check_type": r.get("check_type"),
            "query_text": r.get("query_text"),
            "expected_output_format": r.get("expected_output_format"),
            "table_catalog": r.get("table_catalog"),
            "table_schema": r.get("table_schema"),
        })

    print(f"[INFO] QA | Rows fetched from qa_query_plan: {len(out)}")
    logging.info("QA | Rows fetched from qa_query_plan: %d", len(out))
    return out


# =============================
# Execute QA queries on SQL
# =============================

class RunQAQuery(beam.DoFn):
    """
    Executes a QA query_text on SQL Server and returns a small result dict.
    Scalar-first; for tabular outputs we sample up to 1000 rows.
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
            "expected_output_format": qa_row.get("expected_output_format"),
            "status": "OK",
            "value": None,
            "error": None,
            "executed_at_utc": datetime.utcnow().isoformat()
        }
        try:
            cur = self.conn.cursor()
            cur.execute(qa_row["query_text"])
            row = cur.fetchone()
            # Scalar: either no description or single column
            if row is not None and (cur.description is None or len(cur.description) == 1):
                out["value"] = row[0]
            else:
                cols = [d[0] for d in cur.description] if cur.description else []
                rows = []
                if cols:
                    for i, r in enumerate(cur.fetchmany(1000)):
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


# =============================
# Build EXPECTED map from QA results
# =============================

class ExpectedCombiner(beam.CombineFn):
    """
    CombineFn that reduces QA SQL results into a single dict expected_map:
      - "count_total_rows" -> int
      - "nulls_per_column|<snake_case_col>" -> int
    """
    def create_accumulator(self):
        return {}

    def add_input(self, accumulator: Dict, rec: Dict):
        ctype = (rec.get("check_type") or "").lower()
        val = rec.get("value")

        if ctype == "count_total_rows":
            if isinstance(val, (int, float)):
                accumulator["count_total_rows"] = _safe_int(val)
            elif isinstance(val, list) and val and isinstance(val[0], dict):
                # try common keys
                for k, v in val[0].items():
                    if str(k).lower() in ("total_rows", "count", "cnt"):
                        accumulator["count_total_rows"] = _safe_int(v)
                        break

        elif ctype == "nulls_per_column":
            # Expect single-row dict with many "nulls_<Col>" fields
            row_dict = None
            if isinstance(val, list) and val and isinstance(val[0], dict):
                row_dict = val[0]
            elif isinstance(val, dict):
                row_dict = val

            if row_dict:
                for k, v in row_dict.items():
                    k_str = str(k)
                    if k_str.lower().startswith("nulls_"):
                        raw_col = k_str[6:]
                        norm_col = camel_to_snake_case(raw_col)
                        key = f"nulls_per_column|{norm_col}"
                        accumulator[key] = _safe_int(v)

        return accumulator

    def merge_accumulators(self, accumulators):
        merged = {}
        for acc in accumulators:
            for k, v in acc.items():
                # For duplicates we take the last (shouldn't happen, but keep simple)
                merged[k] = v
        return merged

    def extract_output(self, accumulator):
        return accumulator


# =============================
# Read Parquet and compute OBSERVED metrics
# =============================

class ParquetShardMetrics(beam.DoFn):
    """
    Reads a Parquet shard from GCS and computes:
      - count_total_rows
      - nulls_per_column|<field> (sum of nulls per column in the shard)
    Emits a dict with these metrics for the shard.
    """
    def __init__(self):
        self.fs = gcsfs.GCSFileSystem()

    def process(self, parquet_path: str):
        metrics = defaultdict(int)
        try:
            with self.fs.open(parquet_path, 'rb') as f:
                pf = pq.ParquetFile(f)
                for batch in pf.iter_batches():
                    # Count of rows in this batch
                    metrics["count_total_rows"] += batch.num_rows
                    # Null counts per column
                    for arr, field in zip(batch.columns, batch.schema):
                        col_name = camel_to_snake_case(field.name)
                        key = f"nulls_per_column|{col_name}"
                        # arr may not have null_count on chunked arrays; convert to Array
                        pa_arr = pa.array(arr)
                        metrics[key] += int(pa_arr.null_count)
        except Exception as e:
            print(f"[ERROR] Failed reading parquet {parquet_path}: {e}")
            logging.error(f"Failed reading parquet {parquet_path}: {e}")
        yield dict(metrics)


class MergeMetrics(beam.CombineFn):
    """
    CombineFn that sums integer metrics across shards.
    """
    def create_accumulator(self):
        return defaultdict(int)

    def add_input(self, acc: defaultdict, shard_map: Dict):
        for k, v in shard_map.items():
            acc[k] += _safe_int(v)
        return acc

    def merge_accumulators(self, accs):
        merged = defaultdict(int)
        for acc in accs:
            for k, v in acc.items():
                merged[k] += _safe_int(v)
        return merged

    def extract_output(self, acc: defaultdict):
        return dict(acc)


# =============================
# Compare observed vs expected
# =============================

class CompareObservedExpected(beam.DoFn):
    """
    Compares one observed_map (dict) with a side-input expected_map (dict).
    Emits an alert per key.
    """
    def process(self, observed_map: Dict, expected_map: Dict):
        now_iso = datetime.utcnow().isoformat()
        alerts = []

        for k_obs, v_obs in observed_map.items():
            exp = expected_map.get(k_obs)
            if exp is None:
                alerts.append({
                    "key": k_obs,
                    "status": "MISSING_SQL",
                    "expected": None,
                    "observed": _safe_int(v_obs),
                    "evaluated_at_utc": now_iso,
                })
            else:
                status = "OK" if _safe_int(v_obs) == _safe_int(exp) else "MISMATCH"
                alerts.append({
                    "key": k_obs,
                    "status": status,
                    "expected": _safe_int(exp),
                    "observed": _safe_int(v_obs),
                    "evaluated_at_utc": now_iso,
                })
        # Also, if there are expected keys not present in observed, flag them (optional)
        for k_exp in expected_map.keys():
            if k_exp not in observed_map:
                alerts.append({
                    "key": k_exp,
                    "status": "MISSING_PARQUET",
                    "expected": _safe_int(expected_map[k_exp]),
                    "observed": None,
                    "evaluated_at_utc": now_iso,
                })

        for a in alerts:
            yield a


# =============================
# Pipeline
# =============================

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
    custom = pipeline_options.view_as(CustomPipelineOptions)
    print(custom)

    # Build SQL Server connection string
    config = get_sql_config(custom.secret_id, custom.gcp_project)
    connection_string = build_connection_string(config)

    # Optional: BQ max change version used in extraction branch
    bq_table = f"{custom.database.lower()}_{custom.schema.lower()}_{custom.table.lower()}"
    bq_dataset = "sqlserver_to_bq_silver"
    max_version_bq = get_max_sys_change_version_bq(custom.gcp_project, bq_dataset, bq_table)
    print(f"[INFO] Max sys_change_version from BigQuery: {max_version_bq}")

    # Read QA plan rows
    qa_check_types = _parse_qa_check_types(getattr(custom, "qa_check_types", ""))
    print(f"[INFO] QA check types: {qa_check_types} ({type(qa_check_types)})")

    qa_rows = read_qa_query_plan(
        gcp_project=custom.gcp_project,
        qa_plan_dataset=custom.qa_plan_dataset,
        qa_plan_table=custom.qa_plan_table,
        database=custom.database,
        schema=custom.schema,
        table=custom.table,
        check_types=qa_check_types
    )
    print(f"[INFO] QA | Total QA definitions found: {len(qa_rows)}")

    # Generate extraction queries
    queries = generate_batch_queries(
        database=custom.database,
        schema=custom.schema,
        table=custom.table,
        batch_size=custom.batch_size,
        connection_string=connection_string,
        reload_flag=custom.reload_flag.strip().lower() in ("true", "1", "yes"),
        type_of_extraction=custom.type_of_extraction,
        primary_key=custom.primary_key,
        min_version_bq=max_version_bq
    )
    print(f"[INFO] Total extraction batch queries: {len(queries)}")

    # Output prefixes
    timestamp_prefix = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
    qa_prefix = os.path.join(custom.output_gcs_path, "qa_results", timestamp_prefix)
    alerts_prefix = os.path.join(custom.output_gcs_path, "qa_alerts", timestamp_prefix)

    with beam.Pipeline(options=pipeline_options) as p:
        # -------------------------
        # Branch A: Extraction → Parquet paths
        # -------------------------
        parquet_paths = (
            p
            | "A.Create Batch Queries" >> beam.Create(queries)
            | "A.Read & Chunk SQL" >> beam.ParDo(StreamedChunkedReader(connection_string, custom.chunk_size))
            | "A.Write Parquet" >> beam.ParDo(WriteParquetPerBatch(custom.output_gcs_path))
        )

        # Compute observed metrics from Parquet shards and combine globally to a single dict
        observed_map_pc = (
            parquet_paths
            | "A.Read Parquet & Shard Metrics" >> beam.ParDo(ParquetShardMetrics())
            | "A.Merge Metrics" >> beam.CombineGlobally(MergeMetrics()).without_defaults()
        )

        # -------------------------
        # Branch B: Run QA SQL → Expected map
        # -------------------------
        expected_map_pc = (
            p
            | "B.Create QA Rows" >> beam.Create(qa_rows)
            | "B.Run QA Queries" >> beam.ParDo(RunQAQuery(connection_string))
            | "B.Combine to Expected Map" >> beam.CombineGlobally(ExpectedCombiner()).without_defaults()
        )

        # Optional: persist raw QA results for debugging
        _ = (
            p
            | "B.Create QA Rows copy" >> beam.Create(qa_rows)
            | "B.Run QA Queries copy" >> beam.ParDo(RunQAQuery(connection_string))
            | "B.QA JSONL" >> beam.Map(json.dumps)
            | "B.Write QA JSONL" >> beam.io.WriteToText(qa_prefix, file_name_suffix=".jsonl", num_shards=1)
        )

        # -------------------------
        # Compare observed vs expected
        # -------------------------
        alerts = (
            observed_map_pc
            | "C.Compare Obs vs Exp" >> beam.ParDo(
                CompareObservedExpected(),
                expected_map=beam.pvalue.AsSingleton(expected_map_pc)
            )
            | "C.To JSON" >> beam.Map(json.dumps)
            | "C.Write Alerts" >> beam.io.WriteToText(alerts_prefix, file_name_suffix=".jsonl", num_shards=1)
        )

    print("[INFO] Pipeline finished.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run_pipeline()


⸻

Cómo valida este código tu problema de MISSING_SQL
	•	En ExpectedCombiner.add_input ahora “explota” la fila de nulls_per_column leyendo cada campo nulls_Xxx → normaliza a snake_case (xxx) → crea la clave nulls_per_column|xxx con su valor.
	•	Esas claves sí coinciden con las que produce el lector de Parquet (ParquetShardMetrics), que usa nombres snake_case para columnas.
	•	La comparación en CompareObservedExpected deja de marcar MISSING_SQL (salvo que realmente no exista en el SQL), y te dará OK o MISMATCH.

Si quieres que registre también las claves esperadas que no aparecen en Parquet, ya lo hace con MISSING_PARQUET.

¿Necesitas que convierta las columnas numéricas de Parquet a enteros explícitos o que ignore algunas columnas específicas? Te lo ajusto enseguida.