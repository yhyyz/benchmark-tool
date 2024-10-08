from pyhive import hive
from util.loguru import logger
from engine.engine import Engine
class HiveEngine(Engine):
    def __init__(self, host, database, user, port,auth=None, password=None):
        self.db_config = {
            'host': host,
            'username': user,
            'port': port,
            'auth': auth,
            'password': password,
            'database': database
        }

    def execute_sql(self, sql):
        # connection = hive.connect(host="ip-172-31-129-4",port=10000,username="hadoop",database="tmp_tpcds_3tb",password=None)
        connection = None
        cursor = None
        try:
            connection = hive.connect(**self.db_config)
            logger.info(f"create conn, db config: {self.db_config}")
            cursor = connection.cursor()
            query = """
            -- query1
            select count(1) from web_sales
            """
            cursor.execute(query)
            #cursor.execute(sql.replace(";", ""))
            #with connection.cursor() as cursor:
                #cursor.execute(f"use {self.catalog}.{self.db_config.get('database')};")
                #cursor.execute("select 3")
                #cursor.execute(sql.replace(";", ""))
                #connection.commit()
        except Exception as e:
            logger.error(f"Error SQL: {sql}, {e}")
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()