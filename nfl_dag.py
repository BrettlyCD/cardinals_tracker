from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.pythonOperator import PythonOperator
from airflow.utils.dates import days_ago

from nfl_etl import run_nfl_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'tutorial',
    default_args=default_args,
    description='Cardinals etl code'
)

run_etl = PythonOperator(
    task_id='complete_nfl_etl',
    python_callable=run_nfl_etl,
    dag=dag
)

run_etl