import textwrap
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

import pendulum

local_tz = pendulum.timezone('Asia/Seoul')
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 9, 8, tzinfo=local_tz),
    'email': ['cookieshake.dev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG('crawl-upbit-candle', 
    default_args=default_args,
    concurrency=1, 
    max_active_runs=1,
    schedule_interval='* * * * *',
    catchup=False
)
with open('/root/airflow-dags/dags/resources/upbit-market') as f:
    upbit_market = json.loads(f.read())

for market in [m['market'] for m in upbit_market if m['market'].startswith('KRW')]:
    crawl = DockerOperator(
        dag=dag,
        force_pull=True,
        task_id='crawl_' + market.lower().replace('-', '_'),
        image='ingtranet/crawling',
        api_version='auto',
        auto_remove=True,
        command=textwrap.dedent("""
            upbit_candle -a market={}
        """.format(market)),
        environment={
            'MONGO_HOST': 'mongodb.mrnet:27017'
        }
    )
