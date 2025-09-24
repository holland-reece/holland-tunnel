# Hello DAG
# initial DAG so UI shows something as I develop the infra

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["day0"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> end
