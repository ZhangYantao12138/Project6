from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from app.ingest_web_priority import main as ingest_main
from app.quality_check import main as quality_check_main
from app.curate import main as curate_main


with DAG(
    dag_id="traffic_snapshot_web_priority_pipeline",
    max_active_runs=1,
    start_date=datetime(2026, 4, 19),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["traffic", "cctv", "snapshot", "web", "priority"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_priority_images",
        python_callable=ingest_main,
    )

    quality_check_task = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check_main,
        op_kwargs={"cycle_minutes": 2},
    )

    curate_task = PythonOperator(
        task_id="curate_hourly_summary",
        python_callable=curate_main,
    )

    ingest_task >> quality_check_task >> curate_task
