from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="elt_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["elt", "telecom"],
) as dag:

    run_elt_pipeline = BashOperator(
        task_id="run_elt_pipeline",
        bash_command="docker exec elt-pipeline-pipeline-1 python app/run_once.py"
    )

    run_elt_pipeline
