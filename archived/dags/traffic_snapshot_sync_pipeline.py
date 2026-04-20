from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from app.ingest_511NY import main as ingest_main
from app.quality_check import main as quality_check_main
from app.curate import main as curate_main


with DAG(
    dag_id="traffic_snapshot_pipeline",
    start_date=datetime(2026, 4, 15),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["traffic", "cctv", "snapshot"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_511ny_images",
        python_callable=ingest_main,
    )

    quality_check_task = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check_main,
    )

    curate_task = PythonOperator(
        task_id="curate_hourly_summary",
        python_callable=curate_main,
    )

    ingest_task >> quality_check_task >> curate_task