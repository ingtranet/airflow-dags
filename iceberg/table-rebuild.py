
from pendulum import datetime
from datetime import timedelta
from textwrap import dedent
from dataclasses import dataclass

from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

from tools.operators import KyuubiOperator

with DAG(
    'table-rebuild',
    start_date=datetime(2022, 4, 9, tz='Asia/Seoul'),
    schedule_interval='0 1 * * *',
    max_active_runs=1,
    max_active_tasks=1,
    catchup=True,
    default_args={
        'execution_timeout': timedelta(minutes=60),
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    }
) as dag:
    KyuubiOperator(
        task_id=f'rewrite',
        sql=dedent("""
            CALL iceberg.system.rewrite_data_files(table => 'twitter.sampled_stream', where => "DATE '{{ data_interval_start | ds }}' <= created_at_ts AND created_at_ts < DATE '{{ data_interval_end | ds }}'")
        """)
    )
