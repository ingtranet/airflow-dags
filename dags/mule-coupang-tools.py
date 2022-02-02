from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

import pendulum

local_tz = pendulum.timezone('Asia/Seoul')
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 12, 16, tzinfo=local_tz),
    'email': ['cookieshake.dev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG('mule-coupang-tools', 
    default_args=default_args,
    concurrency=1, 
    max_active_runs=1,
    schedule_interval='30 */6 * * *',
    catchup=False
)

goldbox = DockerOperator(
    task_id='goldbox',
    force_pull=True,
    image='harbor.ingtra.net/library/mule-coupang-tools',
    api_version='auto',
    auto_remove=True,
    command='goldbox',
    environment={
        'MONGO_URL': 'mongodb://mongodb:27017'
    },
    dag=dag,
)

best_categories = DockerOperator(
    task_id='best_categories',
    force_pull=True,
    image='harbor.ingtra.net/library/mule-coupang-tools',
    api_version='auto',
    auto_remove=True,
    command='best_categories',
    environment={
        'MONGO_URL': 'mongodb://mongodb:27017'
    },
    dag=dag,
)

update_searches = DockerOperator(
    task_id='update_searches',
    force_pull=True,
    image='harbor.ingtra.net/library/mule-coupang-tools',
    api_version='auto',
    auto_remove=True,
    command='update_searches',
    environment={
        'MONGO_URL': 'mongodb://mongodb:27017'
    },
    dag=dag,
)

recommend = DockerOperator(
    task_id='recommend',
    force_pull=True,
    image='harbor.ingtra.net/library/mule-coupang-tools',
    api_version='auto',
    auto_remove=True,
    command='recommend',
    environment={
        'MONGO_URL': 'mongodb://mongodb:27017'
    },
    dag=dag,
)

update_click_urls = DockerOperator(
    task_id='update_click_urls',
    force_pull=True,
    image='harbor.ingtra.net/library/mule-coupang-tools',
    api_version='auto',
    auto_remove=True,
    command='update_click_urls',
    environment={
        'MONGO_URL': 'mongodb://mongodb:27017'
    },
    dag=dag,
)

update_products = DockerOperator(
    task_id='update_products',
    force_pull=True,
    image='harbor.ingtra.net/library/mule-coupang-tools',
    api_version='auto',
    auto_remove=True,
    command='update_products',
    environment={
        'MONGO_URL': 'mongodb://mongodb:27017'
    },
    dag=dag,
)

goldbox >> best_categories >> update_searches >> recommend >> update_click_urls >> update_products
