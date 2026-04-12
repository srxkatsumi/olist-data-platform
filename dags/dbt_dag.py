from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ============================================================
# DBT DAG
# Quem executa: Airflow triggera → dbt roda → Snowflake processa
# Quem armazena: Snowflake
# Task 1 (staging) deve passar para Task 2 (marts) rodar!!
# ============================================================

with DAG(
    dag_id='dbt_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # triggera manualmente ou pelo Airbyte
    catchup=False,
    tags=['dbt', 'snowflake'],
) as dag:

    # TASK 1: roda staging
    # marts SÓ rodam se essa passar!!
    task_staging = BashOperator(
    task_id='dbt_staging',
    bash_command='cd /opt/airflow/dbt/olist && dbt run --select staging --profiles-dir /opt/airflow/dbt/olist',
    )

    task_marts = BashOperator(
        task_id='dbt_marts',
        bash_command='cd /opt/airflow/dbt/olist && dbt run --select marts --profiles-dir /opt/airflow/dbt/olist',
    )
    # DEPENDÊNCIA: staging >> marts
    # >> significa "marts depende de staging"
    task_staging >> task_marts

