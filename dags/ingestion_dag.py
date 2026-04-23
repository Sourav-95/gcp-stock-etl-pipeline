# dags/ingestion_dag.py
# Runs daily. Triggers the ingestion script on Compute Engine via SSH/BashOperator.
# On success, ETL_Orchestrate.py's ExternalTaskSensor unblocks the next stage.

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="yfinance_ingestion",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:

    run_ingestion = BashOperator(
        task_id="run_ingestion",
        bash_command="python3 /path/to/data_ingestion/main.py"
    )
