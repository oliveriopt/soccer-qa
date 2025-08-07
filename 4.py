gcloud dataflow flex-template run "sql-parquet-batched-job-$(date +%Y%m%d-%H%M%S)"   --template-file-gcs-location "gs://rxo-dataeng-datalake-np-dataflow/templates/sql-parquet-to-gcs-batched.json"   --region "us-central1"   --project "rxo-dataeng-datalake-np"   --service-account-email "ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com"   --subnetwork "https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1"   --disable-public-ips   --staging-location "gs://rxo-dataeng-datalake-np-dataflow/staging"   --temp-location "gs://rxo-dataeng-datalake-np-dataflow/temp"   --parameters "gcp_project=rxo-dataeng-datalake-np,table_name=locale.Address,batch_size=500000,output_path=gs://rxo-dataeng-datalake-np-raw/sql/brokerage-fo/XPOMaster/locale/Address/2025/07/02/,secret_id=rxo-dataeng-datalake-np-brokerage-fo-mssql-xpomaster-uat-creds-connection-string,order_column=AddressId,num_workers=30,max_num_workers=50,autoscaling_algorithm=THROUGHPUT_BASED,machine_type=e2-highmem-4,chunk_size=100000"

gcloud dataflow flex-template run "sql-parquet-batched-job-$(date +%Y%m%d-%H%M%S)"   --template-file-gcs-location "gs://rxo-dataeng-datalake-np-dataflow/templates/op-sql-qa-pqt-to-gcs.json"   --region "us-central1"   --project "rxo-dataeng-datalake-np"   --service-account-email "ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com"   --subnetwork "https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1"   --disable-public-ips   --staging-location "gs://rxo-dataeng-datalake-np-dataflow/staging"   --temp-location "gs://rxo-dataeng-datalake-np-dataflow/temp"   --parameters "gcp_project=rxo-dataeng-datalake-np,table_name=locale.Address,batch_size=500000,output_path=gs://rxo-dataeng-datalake-np-raw/sql/brokerage-fo/XPOMaster/locale/Address/2025/07/02/,secret_id=rxo-dataeng-datalake-np-brokerage-fo-mssql-xpomaster-uat-creds-connection-string,order_column=AddressId,num_workers=30,max_num_workers=50,autoscaling_algorithm=THROUGHPUT_BASED,machine_type=e2-highmem-4,chunk_size=100000"

ERROR: (gcloud.dataflow.flex-template.run) INVALID_ARGUMENT: The template parameters are invalid. Details: 
output_gcs_path: Missing required parameter
schema: Missing required parameter
query: Missing required parameter
primary_key: Missing required parameter
database: Missing required parameter
table: Missing required parameter
type_of_extraction: Missing required parameter
reload_flag: Missing required parameter
table_name: Unrecognized parameter
order_column: Unrecognized parameter
output_path: Unrecognized parameter
- '@type': type.googleapis.com/google.dataflow.v1beta3.InvalidTemplateParameters
  parameterViolations:
  - description: Missing required parameter
    parameter: output_gcs_path
  - description: Missing required parameter
    parameter: schema
  - description: Missing required parameter
    parameter: query
  - description: Missing required parameter
    parameter: primary_key
  - description: Missing required parameter
    parameter: database
  - description: Missing required parameter
    parameter: table
  - description: Missing required parameter
    parameter: type_of_extraction
  - description: Missing required parameter
    parameter: reload_flag
  - description: Unrecognized parameter
    parameter: table_name
  - description: Unrecognized parameter
    parameter: order_column
  - description: Unrecognized parameter
    parameter: output_path


gcloud dataflow flex-template run "sql-parquet-batched-job-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location "gs://rxo-dataeng-datalake-np-dataflow/templates/op-sql-qa-pqt-to-gcs.json" \
  --region "us-central1" \
  --project "rxo-dataeng-datalake-np" \
  --service-account-email "ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com" \
  --subnetwork "https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1" \
  --disable-public-ips \
  --staging-location "gs://rxo-dataeng-datalake-np-dataflow/staging" \
  --temp-location "gs://rxo-dataeng-datalake-np-dataflow/temp" \
  --parameters "gcp_project=rxo-dataeng-datalake-np,\
database=XPOMaster,\
schema=locale,\
table=Address,\
query=SELECT * FROM locale.Address,\
primary_key=AddressId,\
output_gcs_path=gs://rxo-dataeng-datalake-np-raw/sql/brokerage-fo/XPOMaster/locale/Address/2025/07/02/,\
type_of_extraction=full,\
reload_flag=true,\
secret_id=rxo-dataeng-datalake-np-brokerage-fo-mssql-xpomaster-uat-creds-connection-string,\
batch_size=500000,\
chunk_size=100000"

