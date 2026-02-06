from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.extract import extract_tft_grandmaster_leaderboard


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="riot_tft_grandmaster_leaderboard",
    description="Extract TFT Grandmaster leaderboard from Riot API",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["riot", "extract", "tft"],
) as dag:
    PythonOperator(
        task_id="extract_tft_grandmaster_leaderboard",
        python_callable=extract_tft_grandmaster_leaderboard,
    )
