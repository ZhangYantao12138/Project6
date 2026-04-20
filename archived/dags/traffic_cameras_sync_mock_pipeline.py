from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from app.sync_mock_cameras import main as sync_cameras_main


with DAG(
    dag_id="traffic_camera_catalog_sync_mock",
    start_date=datetime(2026, 4, 15),
    schedule="@daily",
    catchup=False,
    tags=["traffic", "511ny", "catalog", "mock"],
) as dag:

    sync_cameras_task = PythonOperator(
        task_id="sync_mock_cameras",
        python_callable=sync_cameras_main,
    )