from typing import List, Optional


from airflow.providers.postgres.hooks.postgres import PostgresHook

class IngtranetPostgresHook(PostgresHook):
    def get_table_unique_columns(self, table: str, schema: Optional[str] = "public") -> Optional[List[str]]:
        """
        Helper method that returns the table primary key

        :param table: Name of the target table
        :param schema: Name of the target schema, public by default
        :return: Primary key columns list
        :rtype: List[str]
        """
        sql = """
            select kcu.column_name
            from information_schema.table_constraints tco
                    join information_schema.key_column_usage kcu
                        on kcu.constraint_name = tco.constraint_name
                            and kcu.constraint_schema = tco.constraint_schema
                            and kcu.constraint_name = tco.constraint_name
            where tco.constraint_type = 'UNIQUE'
            and kcu.table_schema = %s
            and kcu.table_name = %s
        """
        pk_columns = [row[0] for row in self.get_records(sql, (schema, table))]
        return pk_columns or None