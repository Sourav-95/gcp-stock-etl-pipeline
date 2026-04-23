# Master controller DAG. Runs @daily and orchestrates all 3 sub-DAGs in sequence.
# Uses ExternalTaskSensor to wait for each stage before triggering the next.
# Logs SUCCESS or FAILURE to the BQ control table regardless of outcome.

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="yfinance_etl_controller",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:

    # 1. Block until yfinance_ingestion DAG finishes for today's run
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="yfinance_ingestion",
        external_task_id="run_ingestion",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",      # frees worker slot while waiting
        poke_interval=60
    )

    # 2. Kick off Dataproc PySpark transformation
    trigger_transformation = TriggerDagRunOperator(
        task_id="trigger_transformation",
        trigger_dag_id="yfinance_transformation",
        conf={"date": "{{ ds }}"}   # passes execution date to sub-DAG
    )

    # 3. Wait for transformation to write to BQ flat table
    wait_for_transformation = ExternalTaskSensor(
        task_id="wait_for_transformation",
        external_dag_id="yfinance_transformation",
        external_task_id="run_transformation",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60
    )

    # 4. Kick off BQ MERGE + dimension refresh + views
    trigger_warehousing = TriggerDagRunOperator(
        task_id="trigger_warehousing",
        trigger_dag_id="yfinance_warehousing"
    )

    # 5. Wait for all BQ warehousing tasks to complete
    wait_for_warehousing = ExternalTaskSensor(
        task_id="wait_for_warehousing",
        external_dag_id="yfinance_warehousing",
        external_task_id="create_views",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60
    )

    # 6. Log SUCCESS — runs only if all upstream tasks succeed (default trigger rule)
    log_success = BigQueryInsertJobOperator(
        task_id="log_success",
        configuration={
            "query": {
                "query": """
                    INSERT INTO `your_project.your_dataset.pipeline_control`
                    (dag_id, status, updated_ts)
                    VALUES ('yfinance_etl_controller', 'SUCCESS', CURRENT_TIMESTAMP())
                """,
                "useLegacySql": False
            }
        }
    )

    # 7. Log FAILURE — runs if ANY stage fails (ONE_FAILED trigger rule)
    log_failure = BigQueryInsertJobOperator(
        task_id="log_failure",
        configuration={
            "query": {
                "query": """
                    INSERT INTO `your_project.your_dataset.pipeline_control`
                    (dag_id, status, updated_ts)
                    VALUES ('yfinance_etl_controller', 'FAILED', CURRENT_TIMESTAMP())
                """,
                "useLegacySql": False
            }
        },
        trigger_rule=TriggerRule.ONE_FAILED
    )


    (
        wait_for_ingestion
        >> trigger_transformation
        >> wait_for_transformation
        >> trigger_warehousing
        >> wait_for_warehousing
        >> log_success
    )

    # Failure path — any one of these failing triggers log_failure
    [
        wait_for_ingestion,
        trigger_transformation,
        wait_for_transformation,
        trigger_warehousing,
        wait_for_warehousing,
    ] >> log_failure
