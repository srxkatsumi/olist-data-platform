from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime, timedelta
import requests
import os
import time

AIRBYTE_URL = "http://host.docker.internal:8000"
AIRBYTE_CONNECTION_ID = os.environ.get("AIRBYTE_CONNECTION_ID")
AIRBYTE_CLIENT_ID = os.environ.get("AIRBYTE_CLIENT_ID")
AIRBYTE_CLIENT_SECRET = os.environ.get("AIRBYTE_CLIENT_SECRET")

def get_airbyte_token():
    response = requests.post(
        f"{AIRBYTE_URL}/api/v1/applications/token",
        json={
            "client_id": AIRBYTE_CLIENT_ID,
            "client_secret": AIRBYTE_CLIENT_SECRET
        }
    )
    response.raise_for_status()
    return response.json()["access_token"]

def trigger_airbyte_sync():
    token = get_airbyte_token()
    response = requests.post(
        f"{AIRBYTE_URL}/api/v1/connections/sync",
        headers={"Authorization": f"Bearer {token}"},
        json={"connectionId": AIRBYTE_CONNECTION_ID}
    )
    response.raise_for_status()
    job_id = response.json()["job"]["id"]
    print(f"Airbyte sync triggered — job_id: {job_id}")
    return job_id

def wait_for_airbyte_sync(**context):
    job_id = context["task_instance"].xcom_pull(task_ids="trigger_airbyte")
    token = get_airbyte_token()
    max_attempts = 60
    attempt = 0

    while attempt < max_attempts:
        response = requests.get(
            f"{AIRBYTE_URL}/api/v1/jobs/{job_id}",
            headers={"Authorization": f"Bearer {token}"}
        )
        response.raise_for_status()
        status = response.json()["job"]["status"]
        print(f"Attempt {attempt + 1} — Airbyte job status: {status}")

        if status == "succeeded":
            print("Airbyte sync completed successfully!")
            return
        elif status in ["failed", "cancelled"]:
            raise Exception(f"Airbyte sync failed with status: {status}")

        attempt += 1
        time.sleep(30)

    raise Exception("Airbyte sync timed out after 30 minutes")

default_args = {
    "owner": "vicky",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="integrated_dag",
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=-1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["airbyte", "dbt", "integrated"],
    doc_md="""
    ## Integrated Pipeline DAG

    Orchestrates the full ELT pipeline in sequence:
    1. Triggers Airbyte sync (PostgreSQL → Snowflake)
    2. Waits for sync to complete
    3. Runs dbt transformations (staging → marts)
    4. Runs dbt tests to validate data quality

    Runs daily at 6am. If any step fails, the pipeline stops.
    """,
) as dag:

    trigger_airbyte = PythonOperator(
        task_id="trigger_airbyte",
        python_callable=trigger_airbyte_sync,
    )

    wait_airbyte = PythonOperator(
        task_id="wait_for_airbyte",
        python_callable=wait_for_airbyte_sync,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/olist && dbt run --profiles-dir /opt/airflow/dbt/olist",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/olist && dbt test --profiles-dir /opt/airflow/dbt/olist",
    )

    trigger_airbyte >> wait_airbyte >> dbt_run >> dbt_test
