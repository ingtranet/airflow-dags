from typing import List

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from pendulum import datetime
from datetime import timedelta

with DAG(
    'coupang-get-new-products',
    start_date=datetime(2022, 7, 25, tz='Asia/Seoul'),
    schedule_interval='0 * * * *',
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    default_args={
        'execution_timeout': timedelta(minutes=5),
        'retries': 3,
        'retry_delay': timedelta(hours=1)
    }
) as dag:
    get_new_products = KubernetesPodOperator(
        task_id='get_new_products',
        name='get_new_products',
        namespace='airflow',
        image='harbor.ingtra.net/library/mule-collector:latest',        
        arguments=['get-new-products'],
        do_xcom_push=False
    )