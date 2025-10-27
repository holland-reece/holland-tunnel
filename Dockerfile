# Dockerfile
FROM apache/airflow:3.1.1-python3.10

# Pass/override these if needed
ARG AIRFLOW_VERSION=3.1.1
ARG PYTHON_VERSION=3.10
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Copy requirements into container dir
COPY requirements.txt /requirements.txt

# Install extras **with the Airflow constraints**
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt \
    --constraint "${CONSTRAINT_URL}"
