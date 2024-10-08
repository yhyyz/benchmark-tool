from pyhive import hive
from util.loguru import logger
from engine.engine import Engine
class HiveEngine(Engine):
    def __init__(self, host, database, user, port, password=None):
        self.db_config = {
            'host': host,
            'username': user,
            'port': port,
            'password': password,
            'database': database
        }

    def execute_sql(self, sql):
        # connection = hive.connect(host="ip-172-31-129-4",port=10000,username="hadoop",database="tmp_tpcds_3tb",password=None)
        connection = hive.connect(**self.db_config)
        try:
            with connection.cursor() as cursor:
                #cursor.execute(f"use {self.catalog}.{self.db_config.get('database')};")
                cursor.execute(sql.replace(";", ""))
                #connection.commit()
        except Exception as e:
            logger.error(f"Error SQL: {sql}, {e}")
        finally:
            cursor.close()
            connection.close()