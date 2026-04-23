# Triggered by ETL_Orchestrate.py (schedule_interval=None → never runs on its own).
# Submits the PySpark job to Dataproc, passing the Airflow execution date
# so the job always processes the correct partition instead of the config default.

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime

with DAG(
    dag_id="yfinance_transformation",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    run_transformation = DataprocSubmitJobOperator(
        task_id="run_transformation",
        project_id="your-project-id",
        region="your-region",
        job={
            "placement": {"cluster_name": "your-cluster"},
            "pyspark_job": {
                "main_python_file_uri": "gs://your-bucket/code/data_transformation/main.py",
                # transformer.py must be staged alongside main.py
                "python_file_uris": [
                    "gs://your-bucket/code/data_transformation/transformer.py"
                ],
                # config files are downloaded to Dataproc working dir by Spark --files
                "file_uris": [
                    "gs://your-bucket/code/config/config.yaml",
                    "gs://your-bucket/code/config/schema.yaml"
                ],
                # --date overrides config.yaml runtime.process_date
                "args": ["--date", "{{ ds }}"]
            }
        }
    )
