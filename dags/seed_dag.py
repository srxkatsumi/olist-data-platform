from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

# ============================================================
# CONFIGURAÇÃO
# Quem armazena: PostgreSQL
# Quem executa: pandas + SQLAlchemy
# ============================================================

# Conexão com o PostgreSQL
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/airflow"

# Pasta onde estão os CSVs
DATA_PATH = "/opt/airflow/data"

# ============================================================
# TASKS
# ============================================================

def carregar_csv_no_postgres():
    """
    Task 1: Lê cada CSV da pasta /data
    Task 2: Insere no PostgreSQL
    Quem executa: pandas
    Quem armazena: PostgreSQL
    """
    engine = create_engine(DB_CONN)
    
    # Lista todos os CSVs na pasta
    arquivos = [f for f in os.listdir(DATA_PATH) if f.endswith('.csv')]
    
    for arquivo in arquivos:
        # Nome da tabela = nome do arquivo sem .csv
        tabela = arquivo.replace('.csv', '')
        caminho = f"{DATA_PATH}/{arquivo}"
        
        print(f"Carregando {arquivo} → tabela {tabela}...")
        
        # pandas lê o CSV
        df = pd.read_csv(caminho)
        
        # pandas insere no PostgreSQL
        df.to_sql(
            name=tabela,
            con=engine,
            if_exists='replace',  # substitui se já existir
            index=False
        )
        
        print(f"✅ {tabela}: {len(df)} linhas inseridas!")

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id='seed_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # roda só manualmente!
    catchup=False,
    tags=['seed', 'postgresql'],
) as dag:

    task_carregar = PythonOperator(
        task_id='carregar_csvs_no_postgres',
        python_callable=carregar_csv_no_postgres,
    )
