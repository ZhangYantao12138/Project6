from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from app.feature_extract_mock import main as feature_extract_main


with DAG(
    dag_id="traffic_feature_extract_pipeline",
    max_active_runs=1,
    start_date=datetime(2026, 4, 19),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["traffic", "cctv", "features", "cv"],
) as dag:

    feature_extract_task = PythonOperator(
        task_id="extract_mock_features",
        python_callable=feature_extract_main,
    )
