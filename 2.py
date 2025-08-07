import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyodbc
import pandas as pd
import os

# Define custom pipeline options
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--connection_name', required=True, help='Name of the database connection (e.g., bdw, fo)')
        parser.add_argument('--query', required=True, help='SQL query to execute')
        parser.add_argument('--output_path', required=True, help='GCS path for the Parquet file (e.g., gs://your-bucket/path/file.snappy.parquet)')

# Dictionary of connection strings
CONNECTION_STRINGS = {
    "bdw": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=BrokerageWarehouse-UAT.RXO.COM\BI;DATABASE=WarehouseBrokerage;Trusted_Connection=yes;",
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
            conn.close()
            # Convert rows to dictionaries
            return [dict(zip([column[0] for column in cursor.description], row)) for row in rows]

        return pcoll | beam.FlatMap(fetch_data)

# Custom DoFn to batch rows and write to a single Parquet file
class WriteSingleParquet(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, elements, *args, **kwargs):
        # Convert the list of dictionaries to a Pandas DataFrame
        df = pd.DataFrame(elements)
        # Write the DataFrame to a single Parquet file with Snappy compression
        df.to_parquet(self.output_path, index=False, compression='snappy', storage_options={"token": "cloud"})
        yield self.output_path

# Define the pipeline
def run_pipeline():
    # Parse pipeline options
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Use DataflowRunner for cloud execution
        project='rxo-dataeng-datalake-np',  # Updated project ID
        region='us-central1',
        temp_location='gs://rxo-dataeng-datalake-np-dataflow/temp',  # Updated temp location
        staging_location='gs://rxo-dataeng-datalake-np-dataflow/staging',  # Updated staging location
        # requirements_file='gs://rxo-dataeng-datalake-np-dataflow/scripts/requirements.txt',
        save_main_session=True,
        service_account_email='ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com',
        network='nxo-network-us1',  # Replace with your network name if not using the default
        subnetwork='https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1',  # Replace with your subnetwork name
        use_public_ips=False
    )
    custom_options = pipeline_options.view_as(CustomPipelineOptions)

    # Extract parameters
    connection_name = custom_options.connection_name
    query = custom_options.query
    output_path = custom_options.output_path

    # Get the connection string from the dictionary
    if connection_name not in CONNECTION_STRINGS:
        raise ValueError(f"Unknown connection name: {connection_name}")
    connection_string = CONNECTION_STRINGS[connection_name]

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Start' >> beam.Create([None])  # Create a dummy PCollection
            | 'Query Database' >> QueryDatabase(connection_string=connection_string, query=query)
            | 'Batch Rows' >> beam.combiners.ToList()  # Batch all rows into a single list
            | 'Write to Single Parquet' >> beam.ParDo(WriteSingleParquet(output_path))
        )

# Entry point
if __name__ == '__main__':
    run_pipeline()

'''
Example command to run the script:
python.exe custom_sql_to_parquet.py --connection_name="fo" --query="SELECT TOP 100 * FROM orders.Order" --output_path="gs://rxo-dataeng-datalake-np-raw/sql/load.snappy.parquet"
'''
