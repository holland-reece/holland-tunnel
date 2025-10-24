# Module script for ingrstion dag; not yet implemented

import os
import requests
import json
# import pandas as pd
# import numpy as np
import warnings
# from flatten_json import flatten
# from airflow.providers.standard.operators.python import PythonOperator
# from airflow.utils.context import get_current_context

def fetch_npi_data(**context):
    url = os.getenv("NPI_URL")
    if url == None:
        warnings.warn(f"\nWARNING!!! No NPPES NPI URL indicated!\n")
    data = requests.get(url).json()
    os.makedirs("/tmp/npi", exist_ok=True)   # no error raised if exists
    path = "/tmp/npi/npi_sample.json"
    with open(path, "w") as f:
        json.dump(data, f)
    context['ti'].xcom_push(key='npi_path', value=path)