import textwrap
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

import pendulum

local_tz = pendulum.timezone('Asia/Seoul')
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 2, 5, tzinfo=local_tz),
    'email': ['cookieshake.dev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG('crawl-naver-blog', 
    default_args=default_args,
    concurrency=1, 
    max_active_runs=1,
    schedule_interval='0 */6 * * *',
    catchup=False
)

crawl = DockerOperator(
    dag=dag,
    force_pull=True,
    task_id='crawl',
    image='harbor.ingtra.net/library/crawling',
    api_version='auto',
    auto_remove=True,
    command=textwrap.dedent("""
        naver_blog
    """),
    environment={
        'MONGO_HOST': 'mongodb.mrnet:27017'
    }
)
