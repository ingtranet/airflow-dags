from typing import List

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from pendulum import datetime
from datetime import timedelta

with DAG(
    'coupang-update-prices',
    start_date=datetime(2022, 7, 25, tz='Asia/Seoul'),
    schedule_interval='0 * * * *',
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    default_args={
        'execution_timeout': timedelta(hours=2),
        'retries': 0,
        'retry_delay': timedelta(hours=1)
    }
) as dag:
    update_prices = KubernetesPodOperator(
        task_id='update_prices',
        name='update_prices',
        namespace='airflow',
        image='harbor.ingtra.net/library/mule-collector:latest',        
        arguments=['update-prices', '--count', '3000'],
        do_xcom_push=False
    )