from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'lastfm_dag',
        default_args=default_args,
        description='Dag for lastfm data',
        schedule_interval=None,
        catchup=False
    ) as lastfm_dag:
    
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')
    
    start_task >> end_task

