from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

import pendulum

from datetime import datetime, timedelta

from util import crawl_daumnews

local_tz = pendulum.timezone('Asia/Seoul')
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2000, 1, 1, tzinfo=local_tz),
    'email': ['cookieshake.dev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG('crawl-daum-news-chosun', 
    default_args=default_args,
    concurrency=3, 
    max_active_runs=3,
    schedule_interval='0 2 * * *')

task_crawl = PythonOperator(
    task_id='task_crawl',
    provide_context=True,
    python_callable=crawl_daumnews,
    op_kwargs={'media_code': 200},
    dag=dag)
