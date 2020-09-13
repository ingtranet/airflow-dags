import textwrap
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

import pendulum

local_tz = pendulum.timezone('Asia/Seoul')
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 9, 13, tzinfo=local_tz),
    'email': ['cookieshake.dev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=0)
}

dag = DAG('ping-brood-lord-blogs', 
    default_args=default_args,
    concurrency=3, 
    max_active_runs=1,
    schedule_interval='0 0 * * *',
    catchup=False
)


def ping_url(url):
    r = http.request(
        'GET',
        f'https://www.google.com/ping',
        fields={'sitemap': f'https://{url}/sitemap.xml'}
    )
    assert(r.status == 200)
    print(r.data.decode('utf-8'))
    r = http.request(
        'GET',
        f'http://www.bing.com/ping',
        fields={'sitemap': f'https://{url}/sitemap.xml'}
    )
    assert(r.status == 200)
    print(r.data.decode('utf-8'))

def ping_all(**kwargs):
    url_list = Variable.get('url_list', deserialize_json=True)
    for url in url_list:
        print('ping: ' + url)
        ping_url(url)

crawl = PythonOperator(
    dag=dag,
    task_id='ping_all',
    provide_context=True,
    python_callable=ping_all
)

