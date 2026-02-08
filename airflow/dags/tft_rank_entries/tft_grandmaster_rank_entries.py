from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.extract import (
    extract_tft_grandmaster_leaderboard,
    extract_tft_entries_by_puuid,
)

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
    leaderboard_task = PythonOperator(
        task_id="extract_tft_grandmaster_leaderboard",
        python_callable=extract_tft_grandmaster_leaderboard,
    )

    player_entries_task = PythonOperator(
        task_id="extract_tft_entries_by_puuid",
        python_callable=extract_tft_entries_by_puuid,
        op_kwargs={
            "puuid_file": (
                "{{ ti.xcom_pull(task_ids='extract_tft_grandmaster_leaderboard') }}"
            ),
        },
    )

    leaderboard_task >> player_entries_task
