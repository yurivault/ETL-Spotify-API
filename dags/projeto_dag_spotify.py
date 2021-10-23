from datetime import timedelta
from datetime import *
from airflow import DAG
from airflow import *
from airflow.operators.python_operator import PythonOperator
from projeto_etl_spotify import executar_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 17),
    'email': ['yuri.amaral@ufms.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'projeto_dag_spotify',
    default_args=default_args,
    description='DAG com um processo de ETL para o projeto Spotify',
    schedule_interval=timedelta(days=1),
)

def apenas_uma_funcao():
    print("Espero que dê certo")

iniciar_etl = PythonOperator(
    task_id='projeto_etl_spotify',
    python_callable= executar_etl, 
    dag=dag,
)

iniciar_etl