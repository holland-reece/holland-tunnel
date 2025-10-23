# DAG 0: Hello DAG

# initial DAG so Airflow UI shows something as I develop the infra

from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.datetime import BranchDateTimeOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG


dag1 = DAG(
    dag_id="hello_dag",
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    catchup=False,
    tags=["day0"],
)

start_task1 = EmptyOperator(task_id="start", dag=dag1)
end_task1 = EmptyOperator(task_id="end", dag=dag1)


# # [START howto_branch_datetime_operator]
# empty_task_11 = EmptyOperator(task_id="date_in_range", dag=dag1)
# empty_task_21 = EmptyOperator(task_id="date_outside_range", dag=dag1)

cond1 = BranchDateTimeOperator(
    task_id="datetime_branch",
    follow_task_ids_if_true=["date_in_range"],
    follow_task_ids_if_false=["date_outside_range"],
    target_upper=pendulum.datetime(2025, 12, 7, 15, 0, 0),
    target_lower=pendulum.datetime(2025, 7, 7, 15, 0, 0),
    dag=dag1,
)

# Run empty_task_11 if cond1 executes between 2020-10-10 14:00:00 and 2020-10-10 15:00:00
# cond1 >> [empty_task_11, empty_task_21]
start_task1 >> end_task1

# [END howto_branch_datetime_operator]
