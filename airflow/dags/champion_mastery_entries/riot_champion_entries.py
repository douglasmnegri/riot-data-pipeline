from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.extract import extract_champion_mastery_entries

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="riot_champion_mastery_entries",
    description="Extract champion mastery entries from Riot API",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["riot", "extract", "league"],
) as dag:
    PythonOperator(
        task_id="extract_champion_mastery_entries",
        python_callable=extract_champion_mastery_entries,
        op_kwargs={
            "player_puuid": "rRllmGqPRjUuZqs1Vy0AZKXhiCvRRJoXELVKM0I4-EfDQ98eNIL_R0fIFx6IS5cbGwJ7HSKjWGSqzg",
        },
    )
