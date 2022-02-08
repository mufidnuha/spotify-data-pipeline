from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.spotify_etl import spotify_etl
from src.load_view import load_view_top_five

TOKEN = ""

default_args = {
    'owner': 'mufida',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 24),
    'email': ['mufidanuha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='spotify_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

etl = PythonOperator(
    task_id='spotify_etl_task',
    dag=dag,
    python_callable=spotify_etl,
    op_kwargs={'token':TOKEN}
)

load_view = PythonOperator(
    task_id='load_view_task',
    dag=dag,
    python_callable=load_view_top_five
)

etl >> load_view