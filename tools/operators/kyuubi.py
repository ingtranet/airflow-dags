from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context

from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

class KyuubiOperator(BaseOperator):
    def __init__(self, sql: str, conn_id='kyuubi-default', **kwargs):
        self.sql = sql
        self.conn_id = conn_id
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:
        result = HiveServer2Hook(hiveserver2_conn_id=self.conn_id).get_results(hql=self.sql)
        print(f'Query Result: {result}')