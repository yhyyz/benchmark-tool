from pyhive import hive
from util.loguru import logger
from engine.engine import Engine
from util.sql import remove_sql_comments
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
            sqls = """
            -- query1
            select 1
            -- query2
            """
            sql = remove_sql_comments(sql)
            for q in sql.split(';'):
                # 去除前后的空白字符
                q = q.strip()
                logger.info(q)
                if q:  # 确保查询不是空字符串
                    cursor.execute(q)
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