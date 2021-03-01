from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

import pendulum

local_tz = pendulum.timezone('Asia/Seoul')
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 3, 1, 21, 0, tzinfo=local_tz),
    'email': ['cookieshake.dev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG('mule-data', 
    default_args=default_args,
    concurrency=2, 
    max_active_runs=1,
    schedule_interval='0 * * * *',
    catchup=False
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

IMAGE = 'harbor.ingtra.net/library/mule-data'
MONGO_URL = 'mongodb://mongodb.mrnet:27017'
SERVICES = ['coupang', 'amazon-us']

for SERVICE in SERVICES:
    search_keywords = DockerOperator(
        task_id='search_keywords',
        force_pull=True,
        image=IMAGE,
        api_version='auto',
        auto_remove=True,
        command='search-keywords',
        environment={
            'SERVICE': SERVICE,
            'MONGO_URL': MONGO_URL
        },
        dag=dag,
    )

    recommend_products = DockerOperator(
        task_id='recommend_products',
        force_pull=True,
        image=IMAGE,
        api_version='auto',
        auto_remove=True,
        command='recommend-products',
        environment={
            'SERVICE': SERVICE,
            'MONGO_URL': MONGO_URL
        },
        dag=dag,
    )

    update_products = DockerOperator(
        task_id='update_products',
        force_pull=True,
        image=IMAGE,
        api_version='auto',
        auto_remove=True,
        command='update-products',
        environment={
            'SERVICE': SERVICE,
            'MONGO_URL': MONGO_URL
        },
        dag=dag,
    )

    misc = DockerOperator(
        task_id='misc',
        force_pull=True,
        image=IMAGE,
        api_version='auto',
        auto_remove=True,
        command='misc',
        environment={
            'SERVICE': SERVICE,
            'MONGO_URL': MONGO_URL
        },
        dag=dag,
    )

    start >> search_keywords >> recommend_products >> update_products >> misc >> end
