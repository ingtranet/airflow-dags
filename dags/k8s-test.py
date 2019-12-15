from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
import pendulum

from datetime import datetime, timedelta

local_tz = pendulum.timezone('Asia/Seoul')
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2000, 1, 1, tzinfo=local_tz),
    'email': ['cookieshake.dev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('crawl-daum-news-kyunghyang', 
    default_args=default_args,
    concurrency=3, 
    max_active_runs=3,
    schedule_interval='0 2 * * *')

k = KubernetesPodOperator(
    namespace='airflow',
    image="ubuntu:18.04",
    cmds=["bash", "-cx"],
    arguments=["echo", "10"],
    name="task-test",
    in_cluster=True,
    task_id="task",
    dag=dag)