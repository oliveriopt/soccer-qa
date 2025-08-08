import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyodbc
import pandas as pd
import os
from qa_run_checks import RunQAComparisonsFn

# Define custom pipeline options
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--connection_name', required=True, help='Name of the database connection (e.g., bdw, fo)')
        parser.add_argument('--query', required=True, help='SQL query to execute')
        parser.add_argument('--output_path', required=True, help='GCS path for the Parquet file (e.g., gs://your-bucket/path/file.snappy.parquet)')
        parser.add_argument('--qa_query_id', required=True, help='Unique QA query ID')
        parser.add_argument('--qa_bq_table', required=True, help='BigQuery table for QA comparison')
        parser.add_argument('--qa_check_type', required=True, help='Type of QA check to perform')

# Dictionary of connection strings
CONNECTION_STRINGS = {
    "bdw": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=BrokerageWarehouse-UAT.RXO.COM\\BI;DATABASE=WarehouseBrokerage;Trusted_Connection=yes;",
    "fo": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=fbtdw2090.qaamer.qacorp.xpo.com;DATABASE=XPOMaster;Trusted_Connection=yes;"
}

# Custom PTransform to query the database
class QueryDatabase(beam.PTransform):
    def __init__(self, connection_string, query):
        self.connection_string = connection_string
        self.query = query

    def expand(self, pcoll):
        def fetch_data(_):
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            cursor.execute(self.query)
            rows = cursor.fetchall()
            result = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
            conn.close()
            return result

        return pcoll | beam.FlatMap(fetch_data)

# Custom DoFn to batch rows and write to a single Parquet file
class WriteSingleParquet(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, elements, *args, **kwargs):
        df = pd.DataFrame(elements)
        df.to_parquet(self.output_path, index=False, compression='snappy', storage_options={"token": "cloud"})
        yield self.output_path

# Define the pipeline
def run_pipeline():
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project='rxo-dataeng-datalake-np',
        region='us-central1',
        temp_location='gs://rxo-dataeng-datalake-np-dataflow/temp',
        staging_location='gs://rxo-dataeng-datalake-np-dataflow/staging',
        save_main_session=True,
        service_account_email='ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com',
        network='nxo-network-us1',
        subnetwork='https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1',
        use_public_ips=False
    )
    custom_options = pipeline_options.view_as(CustomPipelineOptions)

    connection_name = custom_options.connection_name
    query = custom_options.query
    output_path = custom_options.output_path
    qa_query_id = custom_options.qa_query_id
    qa_bq_table = custom_options.qa_bq_table
    qa_check_type = custom_options.qa_check_type

    if connection_name not in CONNECTION_STRINGS:
        raise ValueError(f"Unknown connection name: {connection_name}")
    connection_string = CONNECTION_STRINGS[connection_name]

    with beam.Pipeline(options=pipeline_options) as p:
        parquet_path = (
            p
            | 'Start' >> beam.Create([None])
            | 'Query Database' >> QueryDatabase(connection_string=connection_string, query=query)
            | 'Batch Rows' >> beam.combiners.ToList()
            | 'Write to Single Parquet' >> beam.ParDo(WriteSingleParquet(output_path))
        )

        _ = (
            parquet_path
            | 'Run QA Check' >> beam.ParDo(RunQAComparisonsFn(
                project_id='rxo-dataeng-datalake-np',
                bq_table=qa_bq_table,
                qa_query_id=qa_query_id,
                check_type=qa_check_type
            ))
        )

# Entry point
if __name__ == '__main__':
    run_pipeline()
