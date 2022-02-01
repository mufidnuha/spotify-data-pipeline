from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from spotifyetl_airflow.spotify_etl import spotify_etl
from spotifyetl_airflow.load_view import load_view_top_five

token = "BQA2v2KszL7ko3R3PkSc9SgcWAyQC0JKFzh-UiUhJhyWM3cquJgYN6qX32fqkF9aMYMIeRLyYg7ZYqXMMUh2v7LlxuRV94po_SH5VVEV7zKC2v6KSB1BKZhW8NRGxLzaY1EdVrKaTaxV5iqszop8jZYcU5JmDv2lxe3TvrAci3x-qhg"

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
    op_kwargs={'token':token}
)

load_view = PythonOperator(
    task_id='load_view_task',
    dag=dag,
    python_callable=load_view_top_five
)

etl >> load_view