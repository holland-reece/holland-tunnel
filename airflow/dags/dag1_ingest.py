# DAG 1: Ingest NPPES NPI Registry Data

# Created 2025-09-29
# Holland Brown (https://github.com/holland-reece)

# Uses dedicated NPPES NPI Reg Python API to periodically retrieve and
# ingest data about registered physicians in the US.
# (NPPES NPI Registry API: https://npiregistry.cms.hhs.gov/api-page)

# Dumps data into BigQuery data lake (bronze layer).

# Transforms records into FHIR (Fast Healthcare Interoperability 
# Resources) standardized format. 
# (FHIR: https://www.healthit.gov/sites/default/files/2019-08/ONCFHIRFSWhatIsFHIR.pdf)


import time
from airflow import DAG
# from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpdateDatasetOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryUpsertTableOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests, json, os
# from scripts.call_npi_api import fetch_npi_data
import warnings

GCS_BUCKET = os.getenv("GCS_BUCKET", "npi-bronze-bucket")
BQ_DATASET = os.getenv("BIGQUERY_DATASET", "bronze")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

def fetch_npi_data(**context):
    url = os.getenv("NPI_URL")
    if url == None:
        warnings.warn(f"\nWARNING!!! No NPPES NPI URL indicated!\n")
    data = requests.get(url) #.json()
    os.makedirs(f"{os.getcwd()}/tmp/npi", exist_ok=True)   # no error raised if exists
    path = f"{os.getcwd()}/tmp/npi/npi_sample.json"
    with open(path, "w") as f:
        json.dump(data, f)
    context['ti'].xcom_push(key='npi_path', value=path)
    
with DAG(
    "npi_ingest",
    start_date=datetime(2025, 10, 23),
    schedule=None,
    catchup=False,
    tags=["npi", "demo"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_npi_data",
        python_callable=fetch_npi_data,
    )

    # upload = LocalFilesystemToGCSOperator(
    #     task_id="upload_to_gcs",
    #     src="{{ ti.xcom_pull(task_ids='fetch_npi_data', key='npi_path') }}",
    #     dst="bronze/npi_sample.json",
    #     bucket=GCS_BUCKET,
    # )

    # load_to_bq = BigQueryInsertJobOperator(
    #     # Load data from GCS bucket to BQ dataset
    #     task_id="load_to_bigquery",
    #     configuration={
    #         "load": {
    #             "sourceUris": [f"gs://{GCS_BUCKET}/bronze/npi_sample.json"],
    #             "destinationTable": {
    #                 "projectId": PROJECT_ID,
    #                 "datasetId": BQ_DATASET,
    #                 "tableId": "npi_raw"
    #             },
    #             "sourceFormat": "NEWLINE_DELIMITED_JSON",
    #             "autodetect": True,
    #             "writeDisposition": "WRITE_TRUNCATE",
    #         }
    #     }
    # )

    update_dataset = BigQueryUpdateDatasetOperator(
        task_id="update_dataset",
        dataset_id=BQ_DATASET,
        dataset_resource={"description": "Updated dataset"},
    )

    # # "upsert" table : update and insert; don't overwrite existing data
    # upsert = BigQueryUpsertTableOperator(
    #     task_id="upsert_table",
    #     dataset_id=BQ_DATASET,
    #     table_resource={
    #         "tableReference": {"tableId": "test_table_id"},
    #         "expirationTime": (int(time.time()) + 300) * 1000,
    #     },
    # )

    # fetch >> upload >> load_to_bq
    fetch >> update_dataset

