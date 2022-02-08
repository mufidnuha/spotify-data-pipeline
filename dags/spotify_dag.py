from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.spotify_etl import spotify_etl
from etl.load_view import load_view_top_five

token = "BQBR2MN6YoUxnorvb7t_bZetO5FKRZH2EsDCQVujj1m945PFEzfGugPxwn-5V0iOzIfDUzINieKdCdhp9OekuAJOKpvBDfcAS_S_y-S5R8WUcTiOGC8nEzNJ_qjXv2-o4W5d_GvdlhT8KTQpaqbCFr-v6qtjCo1z00mmTLh3IDXvkBU"

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