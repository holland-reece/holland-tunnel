# DAG 1: Ingest NPPES NPI Registry Data

# Created 2025-09-29
# Holland Brown (https://github.com/holland-reece)

# Uses dedicated NPPES NPI Reg Python API to periodically retrieve and
# ingest data about registered physicians in the US.
# (NPPES NPI Registry API: https://npiregistry.cms.hhs.gov/api-page)

# Dumps data into MinIO (AWS S3 bucket alternative) bronze layer.

# Transforms records into FHIR (Fast Healthcare Interoperability 
# Resources) standardized format. 
# (FHIR: https://www.healthit.gov/sites/default/files/2019-08/ONCFHIRFSWhatIsFHIR.pdf)



from __future__ import annotations

import os
import io
import json
import math
import time
import logging
from datetime import datetime, timedelta

import requests
import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context

# Pydantic v2 + fhir.resources
from pydantic import BaseModel, ValidationError
from fhir.resources.practitioner import Practitioner
from fhir.resources.organization import Organization
from fhir.resources.humanname import HumanName
from fhir.resources.meta import Meta
from fhir.resources.identifier import Identifier
from fhir.resources.address import Address
from fhir.resources.contactpoint import ContactPoint

# -------------------------
# Config (env or defaults)
# -------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")

NPI_API_BASE = os.getenv("NPI_API_BASE", "https://npiregistry.cms.hhs.gov/api/")
NPI_API_VERSION = os.getenv("NPI_API_VERSION", "2.1")
NPI_PAGE_SIZE = int(os.getenv("NPI_PAGE_SIZE", "200"))   # max 200 per docs
NPI_MAX_PAGES = int(os.getenv("NPI_MAX_PAGES", "0"))     # 0 = no cap
NPI_MAX_RPS = float(os.getenv("NPI_MAX_RPS", "3"))       # be polite

# Optional query filters to narrow scope (helps rate limiting & job size)
# e.g., "state=CA", "taxonomy=207Q00000X"
NPI_QUERY_FILTERS = os.getenv("NPI_QUERY_FILTERS", "")   # raw querystring, optional


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )

def s3_key_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404 or e.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise

def put_s3_text(client, bucket: str, key: str, text: str, content_type="application/json"):
    client.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type)

def ensure_bucket(client, bucket: str):
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)

def rate_limit_sleep(last_call_ts: list[float]):
    """Simple token-bucket-ish: enforce max requests per second."""
    if NPI_MAX_RPS <= 0:
        return
    min_interval = 1.0 / NPI_MAX_RPS
    now = time.time()
    if last_call_ts and now - last_call_ts[0] < min_interval:
        time.sleep(min_interval - (now - last_call_ts[0]))
    last_call_ts[:] = [time.time()]

def build_base_params(enumeration_type: str) -> dict:
    """
    enumeration_type: 'NPI-1' (individual) or 'NPI-2' (organization)
    """
    params = {
        "version": NPI_API_VERSION,
        "enumeration_type": enumeration_type,
        "limit": NPI_PAGE_SIZE,
        # we will set skip dynamically
    }
    # Allow optional pass-through filters (like state=CA)
    if NPI_QUERY_FILTERS:
        # crude parse of k=v&k2=v2
        for kv in NPI_QUERY_FILTERS.split("&"):
            if "=" in kv:
                k, v = kv.split("=", 1)
                params[k] = v
    return params

# -------------------------
# NPPES -> FHIR transforms
# -------------------------
NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"

def _extract_phone(address_obj: dict | None) -> str | None:
    if not address_obj:
        return None
    # NPPES may include: telephone_number, practiceLocation phones, etc.
    return address_obj.get("telephone_number") or address_obj.get("practiceLocationPhone")

def _map_addresses(nppes_addresses: list[dict] | None) -> list[Address]:
    addrs: list[Address] = []
    if not nppes_addresses:
        return addrs
    for a in nppes_addresses:
        # Line fields are often 'address_1', 'address_2'
        lines = [x for x in [a.get("address_1"), a.get("address_2")] if x]
        city = a.get("city")
        state = a.get("state")
        postal = a.get("postal_code")
        country = a.get("country_code") or "US"
        use = "work" if (a.get("address_purpose") == "LOCATION") else "work"
        addrs.append(Address.construct(
            line=lines or None,
            city=city,
            state=state,
            postalCode=postal,
            country=country,
            use=use
        ))
    return addrs

def nppes_to_fhir_practitioner(entry: dict) -> Practitioner:
    npi = entry.get("number")
    basic = entry.get("basic") or {}
    # Build HumanName
    name = HumanName.construct(
        family=basic.get("last_name"),
        given=[g for g in [basic.get("first_name"), basic.get("middle_name")] if g] or None,
        prefix=[basic.get("name_prefix")] if basic.get("name_prefix") else None,
        suffix=[basic.get("credential")] if basic.get("credential") else None,
    )
    # Telecom (pick first phone we can find)
    phone = None
    if entry.get("addresses"):
        phone = _extract_phone(entry["addresses"][0])
    telecom = [ContactPoint.construct(system="phone", value=phone)] if phone else None

    return Practitioner(
        id=str(npi),
        meta=Meta(source="NPPES"),
        identifier=[Identifier(system=NPI_SYSTEM, value=str(npi))],
        active=True,
        name=[name],
        address=_map_addresses(entry.get("addresses")),
        telecom=telecom,
    )

def nppes_to_fhir_organization(entry: dict) -> Organization:
    npi = entry.get("number")
    basic = entry.get("basic") or {}
    org_name = basic.get("organization_name") or basic.get("authorized_official_organization_name")
    # Use first address block for telecom
    phone = None
    if entry.get("addresses"):
        phone = _extract_phone(entry["addresses"][0])
    telecom = [ContactPoint.construct(system="phone", value=phone)] if phone else None

    return Organization(
        id=str(npi),
        meta=Meta(source="NPPES"),
        identifier=[Identifier(system=NPI_SYSTEM, value=str(npi))],
        active=True,
        name=org_name,
        address=_map_addresses(entry.get("addresses")),
        telecom=telecom,
    )

# -------------------------
# Extract & Write
# -------------------------
def fetch_and_write(**context: Context):
    """
    Pages NPPES NPI Registry API for both enumeration types and writes
    JSON Lines files to s3://bronze/fhir/<Resource>/dt=YYYY-MM-DD/part-XXXXX.jsonl

    Idempotency:
      - per-run partition path uses Airflow logical date (ds)
      - per-page filenames are deterministic (part-00000.jsonl)
      - if an object exists and SKIP_EXISTING=True, we skip uploading that part
      - writes a _SUCCESS marker when all done
    """
    log = logging.getLogger("nppes_fhir")
    client = s3_client()
    ensure_bucket(client, MINIO_BUCKET)

    ds = context["ds"]  # YYYY-MM-DD (Airflow logical date)
    execution_date = context["logical_date"]  # pendulum dt
    skip_existing = True  # idempotent behavior

    last_call = []  # for rate limiting
    session = requests.Session()
    session.headers.update({"User-Agent": "airflow-nppes-etl/1.0"})

    def process_enum_type(enum_type: str, fhir_resource: str, transform_fn):
        base_params = build_base_params(enum_type)
        skip = 0
        page_idx = 0
        total = None

        # Paths
        prefix = f"fhir/{fhir_resource}/dt={ds}/"
        success_key = prefix + "_SUCCESS"
        if s3_key_exists(client, MINIO_BUCKET, success_key):
            log.info("Partition already marked complete: %s -- skipping.", prefix)
            return

        while True:
            params = dict(base_params)
            params["skip"] = skip
            rate_limit_sleep(last_call)
            resp = session.get(NPI_API_BASE, params=params, timeout=60)
            if resp.status_code != 200:
                raise RuntimeError(f"NPI API {enum_type} HTTP {resp.status_code}: {resp.text}")
            payload = resp.json()

            # Total only available on first page
            if total is None:
                total = payload.get("result_count")  # number returned on this call
                # The API doesn't always give total hits; we'll detect end when zero results.

            results = payload.get("results") or []
            if not results:
                log.info("%s: no more results at skip=%s", enum_type, skip)
                break

            # Validate+transform each result to FHIR, collect JSON lines
            lines = []
            valid_count, invalid_count = 0, 0
            for r in results:
                try:
                    resource_obj = transform_fn(r)
                    # pydantic validation occurs on object instantiation; also call .model_dump()
                    resource_json = resource_obj.model_dump(mode="json", by_alias=True, exclude_none=True)
                    lines.append(json.dumps(resource_json, separators=(",", ":")))
                    valid_count += 1
                except ValidationError as ve:
                    invalid_count += 1
                    log.warning("ValidationError NPI=%s: %s", r.get("number"), ve)

            body = "\n".join(lines) + ("\n" if lines else "")
            part_key = prefix + f"part-{page_idx:05d}.jsonl"

            if skip_existing and s3_key_exists(client, MINIO_BUCKET, part_key):
                log.info("Exists (skip): s3://%s/%s", MINIO_BUCKET, part_key)
            else:
                put_s3_text(client, MINIO_BUCKET, part_key, body, content_type="application/x-ndjson")
                log.info(
                    "Wrote %s (valid=%d invalid=%d, records=%d)",
                    f"s3://{MINIO_BUCKET}/{part_key}",
                    valid_count,
                    invalid_count,
                    len(results),
                )

            # Next page
            page_idx += 1
            skip += NPI_PAGE_SIZE

            if NPI_MAX_PAGES and page_idx >= NPI_MAX_PAGES:
                log.info("Reached NPI_MAX_PAGES=%d for %s", NPI_MAX_PAGES, enum_type)
                break

        # success marker
        put_s3_text(client, MINIO_BUCKET, success_key, "", content_type="text/plain")
        log.info("Wrote success marker: s3://%s/%s", MINIO_BUCKET, success_key)

    # Individuals (NPI-1) -> Practitioner
    process_enum_type("NPI-1", "Practitioner", nppes_to_fhir_practitioner)

    # Organizations (NPI-2) -> Organization
    process_enum_type("NPI-2", "Organization", nppes_to_fhir_organization)


# -----------
# Airflow DAG
# -----------

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nppes_npi_to_minio_fhir",
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["nppes", "npi", "fhir", "bronze", "minio"],
    doc_md="""
Fetch NPPES/NPI Registry data daily, transform to FHIR Practitioner/Organization with pydantic validation,
and write JSON Lines to MinIO in a date-partitioned bronze layout, one file per page, with idempotent behavior.
""",
) as dag:

    extract_nppes = PythonOperator(
        task_id="extract_transform_write",
        python_callable=fetch_and_write,
        provide_context=True,
    )
