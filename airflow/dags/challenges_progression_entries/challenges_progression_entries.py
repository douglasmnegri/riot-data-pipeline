from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.extract import extract_progressed_challenges

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="riot_progressed_challenges",
    description="Extract progressed challenges from Riot API",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["riot", "extract", "league"],
) as dag:
    PythonOperator(
        task_id="extract_progressed_challenges",
        python_callable=extract_progressed_challenges,
        op_kwargs={
            "player_puuid": None,
        },
    )
