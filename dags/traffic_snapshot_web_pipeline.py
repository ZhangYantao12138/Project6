import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from app.ingest_web import main_batch
from app.quality_check import main as quality_check_main
from app.curate import main as curate_main

INGEST_BATCH_COUNT = int(os.getenv("INGEST_BATCH_COUNT", "3"))
CAMERAS_PER_BATCH = int(os.getenv("CAMERAS_PER_BATCH", "100"))

with DAG(
    dag_id="traffic_snapshot_web_pipeline",
    start_date=datetime(2026, 4, 19),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["traffic", "cctv", "snapshot", "web"],
) as dag:

    ingest_tasks = []
    for i in range(INGEST_BATCH_COUNT):
        batch_config = {
            "batch_id": i,
            "offset": i * CAMERAS_PER_BATCH,
            "limit": CAMERAS_PER_BATCH,
        }
        t = PythonOperator(
            task_id=f"ingest_batch_{i}",
            python_callable=main_batch,
            op_kwargs={"batch_config": batch_config},
        )
        ingest_tasks.append(t)

    quality_check_task = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check_main,
    )

    curate_task = PythonOperator(
        task_id="curate_hourly_summary",
        python_callable=curate_main,
    )

    ingest_tasks >> quality_check_task >> curate_task
