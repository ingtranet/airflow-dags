from typing import Any, Sequence
import time

from airflow.models import BaseOperator
from airflow.utils.context import Context

from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

from pyhive.hive import Connection
from TCLIService.ttypes import TOperationState

class KyuubiOperator(BaseOperator):
    template_fields: Sequence[str] = (
        'sql',
    )

    def __init__(self, sql: str, conn_id='kyuubi_default', **kwargs):
        self.sql = sql
        self.conn_id = conn_id
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:
        result = list()
        with HiveServer2Hook(hiveserver2_conn_id=self.conn_id).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(self.sql, async_=True)
                while cursor.poll().operationState in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                    logs = cursor.fetch_logs()
                    for message in logs:
                        print(message)
                    time.sleep(3)
                logs = cursor.fetch_logs()
                for message in logs:
                    print(message)
                final_state = cursor.poll().operationState
                print(f'Final State: {final_state}')
                if final_state != TOperationState.FINISHED_STATE:
                    raise RuntimeError(f'Query Failed {cursor.poll()}')

                columnNames = [column[0] for column in cursor.description]
                records = cursor.fetchall()
                for record in records:
                    result.append(dict(zip(columnNames, record)))
        print(f'Query Result: {result}')