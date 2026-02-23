from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.extract import extract_tft_challenger_leaderboard

from utils.blob_uploader import upload_all_raw_data_to_blob


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="riot_tft_challenger_leaderboard",
    description="Extract TFT Challenger leaderboard from Riot API",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["riot", "extract", "tft"],
) as dag:
    extract_challenger_leaderboard = PythonOperator(
        task_id="extract_tft_challenger_leaderboard",
        python_callable=extract_tft_challenger_leaderboard,
    )

    upload_task = PythonOperator(
        task_id="upload_to_blob",
        python_callable=upload_all_raw_data_to_blob,
    )

    extract_challenger_leaderboard >> upload_task
