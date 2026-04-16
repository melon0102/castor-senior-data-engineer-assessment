
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.extract_csv_to_stage_operator import ExtractCsvToStageOperator
from plugins.operators.data_quality_operator import DataQualityOperator
from plugins.operators.upsert_core_operator import UpsertCoreOperator


def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(
        f"SLA missed for DAG={dag.dag_id}, tasks={task_list}, blocking={blocking_task_list}"
    )


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}

with DAG(
    dag_id="telemetry_csv_to_postgres",
    description="Incremental telemetry pipeline with DQ, idempotency, and observability",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval="0 4 * * *",
    catchup=True,
    max_active_runs=1,
    sla_miss_callback=sla_miss_alert,
    tags=["castor", "dwh", "telemetry"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_to_stage = ExtractCsvToStageOperator(
        task_id="extract_csv_to_stage",
        postgres_conn_id="postgres_dwh",
        file_path="/opt/airflow/sample_data/telemetry_{{ ds }}.csv",
        staging_table="staging.telemetry_raw",
        logical_date="{{ ds }}",
    )

    validate_data = DataQualityOperator(
        task_id="validate_data",
        postgres_conn_id="postgres_dwh",
        logical_date="{{ ds }}",
        source_table="staging.telemetry_raw",
        reject_table="staging.telemetry_rejects",
        max_null_ratio=0.05,
        sla=timedelta(hours=2),
    )

    upsert_core = UpsertCoreOperator(
        task_id="upsert_core",
        postgres_conn_id="postgres_dwh",
        logical_date="{{ ds }}",
        source_table="staging.telemetry_raw",
        target_table="core.telemetry",
        sla=timedelta(hours=2),
    )

    end = EmptyOperator(task_id="end")

    start >> extract_to_stage >> validate_data >> upsert_core >> end