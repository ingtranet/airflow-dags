from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pendulum

from datetime import datetime, timedelta

from util import crawl, temp_json_to_parquet

local_tz = pendulum.timezone('Asia/Seoul')
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 1, 1, tzinfo=local_tz),
    'email': ['cookieshake.dev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('crawl-daum-news-chosun', 
    default_args=default_args,
    concurrency=3, 
    max_active_runs=3,
    schedule_interval='0 2 * * *')

task_crawl = PythonOperator(
    task_id='task_crawl',
    provide_context=True,
    python_callable=crawl,
    op_kwargs={'media_code': 200},
    dag=dag)

task_temp_json_to_parquet = PythonOperator(
    task_id='task_temp_json_to_parquet',
    provide_context=True,
    python_callable=temp_json_to_parquet,
    op_kwargs={'media_code': 200},
    dag=dag)

task_crawl >> task_temp_json_to_parquet