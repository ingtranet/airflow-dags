from typing import List

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pendulum import datetime
from datetime import timedelta
import inflection
import httpx

PARTNERS_API_URL = 'https://coupang-partners-proxy.ig.ingtra.net'

def get_report(report_type:str, **kwargs) -> List[dict]:
    path = {
        'click': 'clicks',
        'order': 'orders',
        'cancel': 'cancels',
        'commission': 'commission'
    }
    with httpx.Client(base_url=PARTNERS_API_URL) as client:
        data = list()
        page = 1
        while True:
            r = client.get(f'/reports/{path[report_type]}', params={
                'startDate': kwargs['data_interval_start'].format('YYYYMMDD'),
                'endDate': kwargs['data_interval_end'].format('YYYYMMDD'),
                'page': page
            })
            print(f'HTTP Request to {r.url}')
            r.raise_for_status()
            d = r.json()['data']
            data.extend(d)
            if len(d) >= 1000:
                page += 1
            else:
                break
        return data

def split_columns_and_rows(data: List[dict]):
    first = data[0]
    columns = [key for key in first.keys()]
    rows = list()
    for d in data:
        row = list()
        for c in columns:
            row.append(d.get(c))
        rows.append(tuple(row))
    columns = [inflection.underscore(col) for col in columns]
    return columns, rows

def get_report_and_insert(report_type:str, **kwargs):
    data = get_report(report_type, **kwargs)
    if not data:
        print('No Data.')
        return
    
    columns, rows = split_columns_and_rows(data)
    hook = PostgresHook()
    schema = 'coupang_partners'
    table = f'{report_type}_report'
    primary_keys = hook.get_table_primary_key(schema=schema, table=table)
    print(f'Table: {table}, Primary Keys: {primary_keys}')
    result = hook.insert_rows(
        table=f'{schema}.{table}',
        rows=rows,
        target_fields=columns,
        replace_index=primary_keys,
        replace=True
    )
    print(result)
    print(f'Inserted {len(rows)} rows.')
    

with DAG(
    "coupang-partners-load-report",
    start_date=datetime(2021, 1, 1, tz="Asia/Seoul"),
    schedule_interval="0 13 1 */3 *",
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "execution_timeout": timedelta(hours=6)
    }
) as dag:
    start = DummyOperator(task_id="start")

    click = PythonOperator(
        task_id='click',
        python_callable=get_report_and_insert,
        op_kwargs={'report_type': 'click'}
    )

    order = PythonOperator(
        task_id='order',
        python_callable=get_report_and_insert,
        op_kwargs={'report_type': 'order'}
    )

    cancel = PythonOperator(
        task_id='cancel',
        python_callable=get_report_and_insert,
        op_kwargs={'report_type': 'cancel'}
    )

    commission = PythonOperator(
        task_id='commission',
        python_callable=get_report_and_insert,
        op_kwargs={'report_type': 'commission'}
    )

    start >> click
    start >> order
    start >> cancel
    start >> commission


