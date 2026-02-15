from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.extract import (
    extract_tft_matches,
    extract_tft_match_details,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="riot_tft_pipeline",
    description="Extract TFT match history and match details from Riot API",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["riot", "tft", "extract"],
) as dag:
    extract_matches_task = PythonOperator(
        task_id="extract_tft_matches",
        python_callable=extract_tft_matches,
        op_kwargs={
            "puuid_dir": "/opt/airflow/data/raw/tft/puuids",
        },
    )

    extract_details_task = PythonOperator(
        task_id="extract_tft_match_details",
        python_callable=extract_tft_match_details,
    )

    extract_matches_task >> extract_details_task
