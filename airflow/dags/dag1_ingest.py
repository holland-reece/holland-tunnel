# DAG 1: Ingest NPPES NPI Registry Data

# Created 2025-09-29
# Holland Brown (https://github.com/holland-reece)

# Uses dedicated NPPES NPI Reg Python API to periodically retrieve and
# ingest data about registered physicians in the US.
# (NPPES NPI Registry API: https://npiregistry.cms.hhs.gov/api-page)

# Dumps data into: (1) GCS bucket, and (2) BigQuery dataset

# BQ dataset contains following tables:
    # npi_raw : landing table
    # npi_providers : cleaned table with only provider info
    # npi_orgs : [maybe will add later] cleaned tab with organizations

# NOTE: Python 3.10 is latest version supported by Airflow


# Import Python packages
import os, json, warnings
from datetime import datetime
import requests
import time

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

# GCS_BUCKET = os.getenv("GCS_BUCKET", "npi-bronze-bucket")
# BQ_DATASET = os.getenv("BIGQUERY_DATASET", "bronze")
# PROJECT_ID = os.getenv("GCP_PROJECT_ID", "holland-tunnel-476017")

# current_date = datetime.today().date()
# formatted_date = current_date.strftime("%Y-%m-%d-%h-%M") # YYYY-MM-DD-HH-MM
# LOCAL_DIR    = os.getenv("LOCAL_TMP_DIR", "/tmp/npi")
# LOCAL_PATH   = os.path.join(LOCAL_DIR, f"{formatted_date}_npi_sample.ndjson")
# os.makedirs(LOCAL_DIR, exist_ok=True)

# GCS_OBJECT   = f"bronze/{formatted_date}_npi_sample.ndjson"
# GCP_CONN_ID  = os.getenv("GCP_CONN_ID", "google_cloud_default")

GCS_BUCKET = os.getenv("GCS_BUCKET", "npi-bronze-bucket")
BQ_DATASET = os.getenv("BIGQUERY_DATASET", "bronze")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "holland-tunnel-476017")
GCP_CONN_ID  = os.getenv("GCP_CONN_ID", "google_cloud_default")

# Use templated names per run; do NOT compute at import-time
LOCAL_DIR = os.getenv("LOCAL_TMP_DIR", "/tmp/npi")
LOCAL_PATH_TMPL = f"{LOCAL_DIR}/npi_{{{{ ds_nodash }}}}.ndjson"
GCS_OBJECT_TMPL = "bronze/npi_{{ ds_nodash }}.ndjson"

print("DEBUG:", GCS_BUCKET, PROJECT_ID, BQ_DATASET)

PCP_TAXONOMY_CODES = {
    "207Q00000X",  # Family Medicine
    "207R00000X",  # Internal Medicine (general)
    "208000000X",  # Pediatrics (general)
    "208D00000X",  # General Practice
    "363LF0000X",  # Nurse Practitioner - Family
    "363LA2200X",  # Nurse Practitioner - Adult Health
    "363LP2300X",  # Nurse Practitioner - Primary Care
}

US_STATES = ["NY", "NJ"]  # states to loop over

PCP_TAXONOMY_DESCS = [
    "Family Medicine",
    "Internal Medicine",
    "Pediatrics",
    "General Practice",
    "Family",          # NP Family
    "Adult Health",    # NP Adult Health
    "Primary Care",    # NP Primary Care
]

# Function to fetch data with NPI Python API
def fetch_npi_data(output_path: str,
                   states=None,
                   taxonomy_descs=None,
                   limit=200,
                   max_pages_per_query=200,     # avoid throttling
                   max_total_records=2000,      # avoid throttling
                   sleep_s=0.15):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    session = requests.Session()
    states = states or US_STATES
    taxonomy_descs = taxonomy_descs or [
        "Family Medicine", "Internal Medicine", "Pediatrics",
        "General Practice", "Family", "Adult Health", "Primary Care",
    ]

    total_seen = total_written = 0
    seen_npi = set()

    with open(output_path, "w") as f:
        for s in states:
            for desc in taxonomy_descs:
                skip = 0
                page = 0
                last_first_npi = None
                while True:
                    page += 1
                    params = {
                        "version": "2.1",
                        "state": s,
                        "enumeration_type": "NPI-1",
                        "taxonomy_description": desc,
                        "limit": limit,
                        "skip": skip,
                    }
                    try:
                        r = session.get("https://npiregistry.cms.hhs.gov/api/", params=params, timeout=60)
                        r.raise_for_status()
                        data = r.json()
                    except Exception as e:
                        print(f"[ERROR] request failed state={s} desc='{desc}' skip={skip}: {e}")
                        break

                    if data.get("Errors"):
                        print(f"[ERROR] API Errors state={s} desc='{desc}' skip={skip}: {data['Errors']}")
                        break

                    results = data.get("results", []) or []
                    total_seen += len(results)
                    print(f"[DEBUG] state={s} desc='{desc}' page={page} got={len(results)} skip={skip}")

                    if not results:
                        break

                    # repeating-page guard
                    first_npi = results[0].get("number") if results and isinstance(results[0], dict) else None
                    if first_npi and first_npi == last_first_npi:
                        print(f"[WARN] repeating page detected; breaking: state={s} desc='{desc}' skip={skip}")
                        break
                    last_first_npi = first_npi

                    for doc in results:
                        # require LOCATION address
                        loc = next((a for a in doc.get("addresses", [])
                                    if (a.get("address_purpose") or "").upper() == "LOCATION"), None)
                        if not loc:
                            continue

                        # keep only PCP taxonomy codes
                        tax = next((t for t in doc.get("taxonomies", [])
                                    if (t.get("code") or "").strip() in PCP_TAXONOMY_CODES), None)
                        if not tax:
                            continue

                        npi = doc.get("number")
                        if not npi or npi in seen_npi:
                            continue
                        seen_npi.add(npi)

                        postal = (loc.get("postal_code") or "").strip()[:5]
                        rec = {
                            "npi": npi,
                            "state": (loc.get("state") or "").strip(),
                            "postal_code": postal,
                            "taxonomy_code": (tax.get("code") or "").strip(),
                            "taxonomy_description": tax.get("desc"),
                        }
                        f.write(json.dumps(rec) + "\n")
                        total_written += 1

                        if total_written >= max_total_records:
                            print(f"[INFO] hit max_total_records={max_total_records}; stopping early")
                            print(f"[SUMMARY] seen={total_seen} written={total_written} path={output_path}")
                            return output_path

                    # paging
                    if len(results) < limit:
                        break  # last page for this (state,desc)
                    if page >= max_pages_per_query:
                        print(f"[INFO] hit max_pages_per_query={max_pages_per_query} for state={s} desc='{desc}'")
                        break
                    skip += limit
                    time.sleep(sleep_s)

    print(f"[SUMMARY] seen={total_seen} written={total_written} path={output_path}")
    return output_path




with DAG(
    dag_id="npi_ingest",
    start_date=datetime(2025, 10, 23),
    schedule="@daily",
    catchup=False,
    tags=["npi", "demo"],
) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_if_missing",
        dataset_id=BQ_DATASET,
        project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
        if_exists="log",
    )

    # 1) CALL the function with the output_path
    # 2) RETURN the path so it's auto-XCom'd as 'return_value'
    def fetch_wrapper(output_path: str, **_):
        path = fetch_npi_data(output_path)
        print(f"[fetch] wrote file: {path}")
        return path

    fetch = PythonOperator(
        task_id="fetch_npi_data",
        python_callable=fetch_wrapper,
        op_kwargs={"output_path": LOCAL_PATH_TMPL},   # templated path per run
    )

    # Pull the return value (no key needed), or explicitly key='return_value'
    upload = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="{{ ti.xcom_pull(task_ids='fetch_npi_data') }}",
        dst=GCS_OBJECT_TMPL,
        bucket=GCS_BUCKET,
        gcp_conn_id=GCP_CONN_ID,
        mime_type="application/x-ndjson",
    )

    load_to_bq = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": [f"gs://{GCS_BUCKET}/" + GCS_OBJECT_TMPL.replace("{{ ds_nodash }}", "{{{{ ds_nodash }}}}")],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId": "npi_raw",
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "autodetect": True,
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "timePartitioning": {"type": "DAY"},
            }
        },
        location=os.getenv("BQ_LOCATION", "US"),
    )

    create_dataset >> fetch >> upload >> load_to_bq
