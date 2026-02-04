import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.extract import (
    extract_rank_entries,
    extract_champion_mastery_entries,
)
from utils.blob_uploader import upload_all_raw_data_to_blob


CONFIG_PATH = "/opt/airflow/dags/rank_entries/configs/rank_entries_jobs.json"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def load_jobs_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


with DAG(
    dag_id="riot_rank_entries",
    description="Extract ranked entries and champion mastery from Riot API",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["riot", "extract", "league"],
) as dag:
    jobs = load_jobs_config(CONFIG_PATH)

    job_end_tasks = []

    for job in jobs:
        rank_task = PythonOperator(
            task_id=f"extract_rank_{job['task_suffix']}",
            python_callable=extract_rank_entries,
            op_kwargs={
                "queue": job["queue"],
                "tier": job["tier"],
                "division": job["division"],
            },
        )

        mastery_task = PythonOperator(
            task_id=f"extract_champion_mastery_{job['task_suffix']}",
            python_callable=extract_champion_mastery_entries,
            op_kwargs={
                "puuid_file": (
                    "{{ ti.xcom_pull(task_ids='extract_rank_"
                    + job["task_suffix"]
                    + "') }}"
                ),
            },
        )

        rank_task >> mastery_task
        job_end_tasks.append(mastery_task)

    upload_task = PythonOperator(
        task_id="upload_all_raw_data_to_blob",
        python_callable=upload_all_raw_data_to_blob,
    )

    job_end_tasks >> upload_task
