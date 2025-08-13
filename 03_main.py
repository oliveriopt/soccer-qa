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
from typing import List, Dict, Optional, Any, Iterable, Tuple


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
            help="List of QA check types separated by '|' (e.g., 'count_total_rows|nulls_per_column|duplicates_primary_key|value_dist_city')."
        )
        parser.add_argument('--qa_plan_dataset', type=str, default="dataops_admin",
                            help="BigQuery dataset for qa_query_plan.")
        parser.add_argument('--qa_plan_table', type=str, default="qa_query_plan",
                            help="Table name of the QA plan in BigQuery.")


def camel_to_snake_case(name: str) -> str:
    """
    Converts CamelCase or PascalCase to snake_case.
    Examples:
      - 'SalespersonID' -> 'salesperson_id'
      - 'createdAt'     -> 'created_at'
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
            # Keep yielding the file path so downstream can compute metrics from it
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

    # NOTE: literal interpolation to keep your style; ensure inputs are trusted.
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


class RunQAQuery(beam.DoFn):
    """
    Executes a QA query_text on SQL Server and returns a small result dict.
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
                    # fetchall could be large; fetchmany with a hard cap
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


# ---------- Parquet metrics computation & comparison ----------

def build_needed_checks(qa_rows: List[Dict], primary_key: str) -> Dict[str, Any]:
    """
    Builds a descriptor of which metrics we need to compute on Parquet,
    normalizing column names to snake_case to match Parquet schema.
    """
    needed = {
        "count_total_rows": False,
        "nulls_per_column": set(),   # empty set => compute for ALL columns seen
        "duplicates_primary_key": False,
        "value_dist": set(),         # snake_case column names
        "primary_key": camel_to_snake_case(primary_key) if primary_key else primary_key,
        "value_dist_top_n": 10
    }
    for r in qa_rows:
        ck = (r.get("check_type") or "").lower()
        if ck == "count_total_rows":
            needed["count_total_rows"] = True
        elif ck == "duplicates_primary_key":
            needed["duplicates_primary_key"] = True
        elif ck == "nulls_per_column":
            # leave empty set to compute all columns
            pass
        elif ck.startswith("nulls_per_column|"):
            col = camel_to_snake_case(ck.split("nulls_per_column|", 1)[1])
            if col:
                needed["nulls_per_column"].add(col)
        elif ck.startswith("value_dist_"):
            col = camel_to_snake_case(ck.split("value_dist_", 1)[1])
            if col:
                needed["value_dist"].add(col)
        elif ck.startswith("value_dist|"):
            col = camel_to_snake_case(ck.split("value_dist|", 1)[1])
            if col:
                needed["value_dist"].add(col)
    return needed


class ComputeMetricsFromParquetFn(beam.DoFn):
    """
    Reads a Parquet file and emits per-file partial metrics as (key, value) pairs:
      - ("count_total_rows", n)
      - ("nulls_per_column|<col>", null_count)
      - ("duplicates_primary_key", num_duplicate_rows)  # rows, not distinct keys
      - ("value_dist|<col>", {value: count, ...})  <-- dict partials, to be merged later
    Later we CombinePerKey to aggregate across many files.
    """
    def __init__(self, needed_checks: Dict[str, Any]):
        self.needed = needed_checks
        self.fs = None

    def setup(self):
        # Prepare GCS filesystem for pyarrow
        self.fs = gcsfs.GCSFileSystem()

    def process(self, parquet_path: str):
        print(f"[INFO] Computing metrics from Parquet: {parquet_path}")
        with self.fs.open(parquet_path, 'rb') as f:
            pf = pq.ParquetFile(f)

            total_rows = 0
            null_counts: Dict[str, int] = {}
            value_freqs: Dict[str, Dict[Any, int]] = {}
            pk_freq: Dict[Any, int] = {}

            pk_name = self.needed.get("primary_key")
            compute_count = bool(self.needed.get("count_total_rows", False))
            compute_dups = bool(self.needed.get("duplicates_primary_key", False)) and bool(pk_name)
            wanted_nulls = self.needed.get("nulls_per_column", set())  # if empty -> all columns
            value_cols = set(self.needed.get("value_dist", set()))
            top_n = int(self.needed.get("value_dist_top_n", 10))

            for rg_index in range(pf.num_row_groups):
                batch_table = pf.read_row_group(rg_index)
                num_rows = batch_table.num_rows
                total_rows += num_rows

                # Initialize null counters lazily based on first row group
                if not null_counts:
                    if wanted_nulls:
                        for name in batch_table.schema.names:
                            if name in wanted_nulls:
                                null_counts[name] = 0
                    else:
                        for name in batch_table.schema.names:
                            null_counts[name] = 0

                # Determine columns we need to scan
                cols_to_scan = set(null_counts.keys())
                if compute_dups and pk_name:
                    cols_to_scan.add(pk_name)
                cols_to_scan |= value_cols

                # Skip if nothing to scan
                if not cols_to_scan:
                    continue

                # Materialize only needed columns
                needed_cols = [c for c in batch_table.schema.names if c in cols_to_scan]
                if not needed_cols:
                    continue

                # Scan needed columns
                for col_name in needed_cols:
                    arr = batch_table.column(col_name)

                    # Null counts
                    if col_name in null_counts:
                        null_counts[col_name] += int(arr.null_count)

                    # Duplicates on PK (count duplicate ROWS within this file)
                    if compute_dups and col_name == pk_name:
                        for chunk in arr.chunks:
                            for v in chunk.to_pylist():
                                pk_freq[v] = pk_freq.get(v, 0) + 1

                    # Value distribution with incremental top-N pruning
                    if col_name in value_cols:
                        if col_name not in value_freqs:
                            value_freqs[col_name] = {}
                        freq = value_freqs[col_name]
                        for chunk in arr.chunks:
                            for v in chunk.to_pylist():
                                freq[v] = freq.get(v, 0) + 1
                                # prune occasionally to keep memory bounded
                                if len(freq) > (top_n * 5):
                                    items = sorted(freq.items(), key=lambda x: x[1], reverse=True)[: top_n * 3]
                                    freq.clear()
                                    freq.update(items)

            # Emit partials
            if compute_count:
                yield ("count_total_rows", total_rows)

            for c, nnulls in null_counts.items():
                yield (f"nulls_per_column|{c}", nnulls)

            if compute_dups and pk_name:
                dup_rows = sum((c - 1) for c in pk_freq.values() if c and c > 1)
                yield ("duplicates_primary_key", dup_rows)

            for c, freq in value_freqs.items():
                yield (f"value_dist|{c}", freq)


def merge_dicts_iter(values: Iterable[Dict[Any, int]]) -> Dict[Any, int]:
    """Merge an iterable of frequency dicts by summing counts (for CombinePerKey)."""
    out: Dict[Any, int] = {}
    for d in values:
        if not d:
            continue
        for k, v in d.items():
            out[k] = out.get(k, 0) + int(v)
    return out


def to_kv_from_qa(qa_row: dict) -> Iterable[Tuple[str, Any]]:
    """
    Converts a QA result (SQL side) into (key, value) pairs.
    Keys are normalized to match Parquet (snake_case for column names).
    """
    ck = (qa_row.get("check_type") or "").lower()
    val = qa_row.get("value")

    # === count_total_rows (scalar) ===
    if ck == "count_total_rows":
        try:
            yield ("count_total_rows", int(val) if val is not None else 0)
        except Exception:
            yield ("count_total_rows", 0)
        return

    # === duplicates_primary_key (scalar OR list-of-dicts) ===
    if ck == "duplicates_primary_key":
        if isinstance(val, list):
            total_dup_rows = 0
            for row in val:
                if isinstance(row, dict):
                    cnt = row.get("cnt")
                    if cnt is None:
                        cnt = row.get("count", 0)
                    try:
                        cnt = int(cnt)
                    except Exception:
                        cnt = 0
                    total_dup_rows += max(cnt - 1, 0)
            yield ("duplicates_primary_key", total_dup_rows)
        else:
            try:
                yield ("duplicates_primary_key", int(val) if val is not None else 0)
            except Exception:
                yield ("duplicates_primary_key", 0)
        return

    # === nulls_per_column (single row with many nulls_* columns) ===
    if ck == "nulls_per_column":
        if isinstance(val, list) and val:
            row0 = val[0]
            if isinstance(row0, dict):
                for k, v in row0.items():
                    if k.lower().startswith("nulls_"):
                        orig_col = k[len("nulls_"):]
                        col = camel_to_snake_case(orig_col)
                        try:
                            yield (f"nulls_per_column|{col}", int(v) if v is not None else 0)
                        except Exception:
                            yield (f"nulls_per_column|{col}", 0)
        elif isinstance(val, dict):
            for k, v in val.items():
                if k.lower().startswith("nulls_"):
                    orig_col = k[len("nulls_"):]
                    col = camel_to_snake_case(orig_col)
                    try:
                        yield (f"nulls_per_column|{col}", int(v) if v is not None else 0)
                    except Exception:
                        yield (f"nulls_per_column|{col}", 0)
        return

    # === nulls_per_column|<col> (explicit) ===
    if ck.startswith("nulls_per_column|"):
        orig_col = ck.split("nulls_per_column|", 1)[1]
        col = camel_to_snake_case(orig_col)
        try:
            yield (f"nulls_per_column|{col}", int(val) if val is not None else 0)
        except Exception:
            yield (f"nulls_per_column|{col}", 0)
        return

    # === value_dist_<col> or value_dist|<col> -> list of (value, count) ===
    if ck.startswith("value_dist_"):
        orig_col = ck.split("value_dist_", 1)[1]
    elif ck.startswith("value_dist|"):
        orig_col = ck.split("value_dist|", 1)[1]
    else:
        orig_col = None

    if orig_col:
        col = camel_to_snake_case(orig_col)
        pairs = []
        if isinstance(val, list):
            for row in val:
                if not isinstance(row, dict):
                    continue
                if col in row and "freq" in row:
                    pairs.append((row[col], int(row["freq"])))
                elif col in row and "count" in row:
                    pairs.append((row[col], int(row["count"])))
                else:
                    items = list(row.items())
                    if len(items) >= 2 and isinstance(items[1][1], (int, float)):
                        pairs.append((items[0][1], int(items[1][1])))
        pairs.sort(key=lambda x: x[1], reverse=True)
        yield (f"value_dist|{col}", pairs)
        return

    # Fallback: unclassified checks are ignored.


def to_kv_from_parquet(metric_row: Tuple[str, Any]) -> Iterable[Tuple[str, Any]]:
    """
    Pass-through: metric_row is already (key, value) from ComputeMetricsFromParquetFn.
    """
    key, value = metric_row
    yield (key, value)


class CompareFn(beam.DoFn):
    """
    Compares expected (SQL) vs observed (Parquet) for each key.
    Emits a dict with status:
      - OK
      - MISMATCH
      - MISSING_SQL
      - MISSING_PARQUET
      - MISSING_BOTH
    """
    def __init__(self, float_tol: float = 0.0, top_n: int = 10):
        self.float_tol = float_tol
        self.top_n = top_n

    def _normalize_value_dist(self, v: Any) -> List[Tuple[Any, int]]:
        """Ensure value distribution is a sorted list[(value,count)] limited to top_n."""
        if v is None:
            return []
        if isinstance(v, dict):
            pairs = [(k, int(v)) for k, v in v.items()]
        elif isinstance(v, list):
            pairs = [(a, int(b)) for (a, b) in v]
        else:
            return []
        pairs.sort(key=lambda x: x[1], reverse=True)
        return pairs[: self.top_n]

    def process(self, kv: Tuple[str, Dict[str, List[Any]]]) -> Iterable[Dict[str, Any]]:
        key, grouped = kv
        sql_vals = grouped.get('sql', [])
        pq_vals = grouped.get('parquet', [])

        result = {
            "key": key,
            "status": "OK",
            "expected": None,   # from SQL (QA)
            "observed": None,   # from Parquet
            "evaluated_at_utc": datetime.utcnow().isoformat()
        }

        result["expected"] = sql_vals[0] if sql_vals else None
        result["observed"] = pq_vals[0] if pq_vals else None

        if result["expected"] is None and result["observed"] is None:
            result["status"] = "MISSING_BOTH"
            yield result
            return
        if result["expected"] is None:
            result["status"] = "MISSING_SQL"
            yield result
            return
        if result["observed"] is None:
            result["status"] = "MISSING_PARQUET"
            yield result
            return

        # Numeric comparison
        if isinstance(result["expected"], (int, float)) and isinstance(result["observed"], (int, float)):
            diff = abs(float(result["expected"]) - float(result["observed"]))
            if diff > self.float_tol:
                result["status"] = "MISMATCH"
            yield result
            return

        # Value distribution comparison
        if key.startswith("value_dist|"):
            exp_pairs = self._normalize_value_dist(result["expected"])
            obs_pairs = self._normalize_value_dist(result["observed"])
            if exp_pairs != obs_pairs:
                result["status"] = "MISMATCH"
            # Store normalized lists for readability
            result["expected"] = exp_pairs
            result["observed"] = obs_pairs
            yield result
            return

        # Fallback exact compare for other types
        if result["expected"] != result["observed"]:
            result["status"] = "MISMATCH"
        yield result


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
    print(qa_rows)
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

    # Build prefixes for QA results and alerts on GCS
    run_ts = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
    qa_prefix = os.path.join(custom_options.output_gcs_path, "qa_results", run_ts)
    alerts_prefix = os.path.join(custom_options.output_gcs_path, "qa_alerts", run_ts)

    # Build needed checks descriptor from qa_rows (used for Parquet metrics)
    needed_checks = build_needed_checks(qa_rows, custom_options.primary_key)
    print(f"[INFO] Needed checks descriptor: {needed_checks}")

    with beam.Pipeline(options=pipeline_options) as p:
        # Branch 1: Original extraction pipeline (unchanged), but capture output file paths
        written_files = (
            p
            | "Create Queries" >> beam.Create(queries)
            | "Read & Chunk SQL" >> beam.ParDo(StreamedChunkedReader(connection_string, custom_options.chunk_size))
            | "Write Parquet" >> beam.ParDo(WriteParquetPerBatch(custom_options.output_gcs_path))
        )

        # Branch 2: QA execution in parallel (SQL side)
        qa_results = (
            p
            | "Create QA Rows" >> beam.Create(qa_rows)
            | "Run QA Queries (SQL Server)" >> beam.ParDo(RunQAQuery(connection_string))
        )

        # Persist QA results to JSONL for audit
        _ = (
            qa_results
            | "QA -> JSON" >> beam.Map(json.dumps)
            | "Write QA JSONL" >> beam.io.WriteToText(qa_prefix, file_name_suffix=".jsonl", num_shards=1)
        )

        # Branch 3: Compute metrics from Parquet (observed side)
        parquet_partials = (
            written_files
            | "Compute Metrics From Parquet" >> beam.ParDo(ComputeMetricsFromParquetFn(needed_checks))
        )

        # Split into numeric vs dict for proper combining:
        numeric_partials = (
            parquet_partials
            | "Filter Numeric Partials" >> beam.Filter(lambda kv: isinstance(kv[1], (int, float)))
        )
        dict_partials = (
            parquet_partials
            | "Filter Dict Partials" >> beam.Filter(lambda kv: isinstance(kv[1], dict))
        )

        parquet_numeric = (
            numeric_partials
            | "Combine Numeric" >> beam.CombinePerKey(sum)
        )
        parquet_freqs_merged = (
            dict_partials
            | "Combine Dicts" >> beam.CombinePerKey(merge_dicts_iter)
            | "Dicts -> TopN Lists" >> beam.Map(
                lambda kv: (
                    kv[0],
                    sorted(kv[1].items(), key=lambda x: x[1], reverse=True)[
                        : int(needed_checks.get("value_dist_top_n", 10))
                    ],
                )
            )
        )

        parquet_metrics = (
            (parquet_numeric, parquet_freqs_merged)
            | "Flatten Parquet Metrics" >> beam.Flatten()
        )

        # Normalize both sides to (key, value)
        qa_kv = (
            qa_results
            | "QA -> KV" >> beam.FlatMap(to_kv_from_qa)
        )
        pq_kv = (
            parquet_metrics
            | "Parquet -> KV" >> beam.FlatMap(to_kv_from_parquet)
        )

        # Join and compare
        grouped = (
            {'sql': qa_kv, 'parquet': pq_kv}
            | "Join SQL vs Parquet" >> beam.CoGroupByKey()
        )

        alerts = (
            grouped
            | "Compare SQL vs Parquet" >> beam.ParDo(CompareFn(float_tol=0.0, top_n=int(needed_checks.get("value_dist_top_n", 10))))
            # To output only mismatches, uncomment the next line:
            # | beam.Filter(lambda d: d["status"] != "OK")
        )

        _ = (
            alerts
            | "Alerts -> JSON" >> beam.Map(json.dumps)
            | "Write Alerts JSONL" >> beam.io.WriteToText(alerts_prefix, file_name_suffix=".jsonl", num_shards=1)
        )

    print("[INFO] Pipeline finished.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run_pipeline()



{"key": "nulls_per_column|zip_id", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.919981"}
{"key": "nulls_per_column|ingestion_datetime_utc", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.920195"}
{"key": "nulls_per_column|sys_change_operation", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.920294"}
{"key": "nulls_per_column|rn", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.920380"}
{"key": "count_total_rows", "status": "OK", "expected": 19195662, "observed": 19195662, "evaluated_at_utc": "2025-08-12T20:56:33.920467"}
{"key": "nulls_per_column|address1", "status": "MISSING_SQL", "expected": null, "observed": 1126, "evaluated_at_utc": "2025-08-12T20:56:33.920554"}
{"key": "nulls_per_column|address_id", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.920635"}
{"key": "nulls_per_column|s2_cell_id", "status": "MISSING_SQL", "expected": null, "observed": 19195662, "evaluated_at_utc": "2025-08-12T20:56:33.920717"}
{"key": "nulls_per_column|geo_code", "status": "MISSING_SQL", "expected": null, "observed": 19195488, "evaluated_at_utc": "2025-08-12T20:56:33.920797"}
{"key": "nulls_per_column|geocoded", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.920877"}
{"key": "nulls_per_column|updated_by", "status": "MISSING_SQL", "expected": null, "observed": 17445269, "evaluated_at_utc": "2025-08-12T20:56:33.920969"}
{"key": "nulls_per_column|country_id", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.921055"}
{"key": "nulls_per_column|city_id", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.921140"}
{"key": "nulls_per_column|latitude", "status": "MISSING_SQL", "expected": null, "observed": 17085379, "evaluated_at_utc": "2025-08-12T20:56:33.921218"}
{"key": "nulls_per_column|longitude", "status": "MISSING_SQL", "expected": null, "observed": 17085381, "evaluated_at_utc": "2025-08-12T20:56:33.921294"}
{"key": "nulls_per_column|updated_date", "status": "MISSING_SQL", "expected": null, "observed": 17502825, "evaluated_at_utc": "2025-08-12T20:56:33.921363"}
{"key": "nulls_per_column|customer_code", "status": "MISSING_SQL", "expected": null, "observed": 19176866, "evaluated_at_utc": "2025-08-12T20:56:33.921425"}
{"key": "nulls_per_column|created_by", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.921488"}
{"key": "nulls_per_column|created_date", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.921559"}
{"key": "nulls_per_column|state_id", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.921620"}
{"key": "nulls_per_column|sys_change_version", "status": "MISSING_SQL", "expected": null, "observed": 0, "evaluated_at_utc": "2025-08-12T20:56:33.921686"}
{"key": "nulls_per_column|address2", "status": "MISSING_SQL", "expected": null, "observed": 17224792, "evaluated_at_utc": "2025-08-12T20:56:33.921742"}
