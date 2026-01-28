from riot.extract import extract_rank_entries
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="riot_rank_entries_challenger",
    description="Extract Challenger ranked entries from Riot API",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["riot", "extract", "league"],
) as dag:
    extract_challenger_entries = PythonOperator(
        task_id="extract_challenger_rank_entries",
        python_callable=extract_rank_entries,
        op_kwargs={
            "queue": "RANKED_SOLO_5x5",
            "tier": "CHALLENGER",
            "division": "I",
        },
    )
