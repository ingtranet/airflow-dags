from typing import Any

from airflow.providers.apache.hive.operators.hive import HiveOperator

class KyuubiOperator(HiveOperator):
    def __init__(self, sql: str, **kwargs: Any) -> None:
        super().__init__(hql=sql, **kwargs)