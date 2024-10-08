import pymysql
import concurrent.futures
import os
import logging
import random
import threading
import time

logging.getLogger().setLevel(logging.INFO)


class StarRocksRunSQL:
    def __init__(self, host, db, user, port, pwd, catalog="default_catalog"):
        self.db_config = {
            'host': host,
            'user': user,
            'port': port,
            'password': pwd,
            'database': db
        }
        self.catalog = catalog
        self.results = {}

    def get_thread_info(self):
        thread_id = threading.get_ident()
        thread_name = threading.current_thread().name
        return thread_id, thread_name

    def execute_sql(self, file_path):
        start_time = int(time.time() * 1000)

        connection = pymysql.connect(**self.db_config)
        try:
            with connection.cursor() as cursor:
                with open(file_path, 'r') as file:
                    sql = file.read()
                    cursor.execute(f"use {self.catalog}.{self.db_config.get('database')};")
                    cursor.execute(sql)
                connection.commit()
        except Exception as e:
            logging.error(f"Error SQL: {file_path}, {e}")
        finally:
            connection.close()
        end_time = int(time.time() * 1000)
        execution_time = end_time - start_time
        sql_name = os.path.basename(file_path).replace(".sql", "")
        if sql_name in self.results:
            self.results[sql_name] = int((self.results[sql_name]+execution_time)/2)
        else:
            self.results[sql_name] = execution_time
        #self.results[sql_name] = execution_time
        thread_id, thread_name = self.get_thread_info()
        logging.info(f"Thread ID: {thread_id}, Thread Name: {thread_name}, SQL: {file_path} Time: {execution_time} ms")

    def extend_array(self,arr, a):
        if a <= len(arr):
            return arr
        extra_count = a - len(arr)
        # 使用循环补充元素
        extended_array = arr + (arr * (extra_count // len(arr))) + arr[:extra_count % len(arr)]
        return extended_array
    def run_in_threads(self, thread_count, sql_directory):
        sql_files = [os.path.join(sql_directory, f) for f in os.listdir(sql_directory) if f.endswith('.sql')]
        random.shuffle(sql_files)
        extended_array = self.extend_array(sql_files, thread_count)
        random.shuffle(extended_array)
        logging.info(f"SQL: {extended_array}")
        start_time = int(time.time() * 1000)
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            executor.map(self.execute_sql, extended_array)
        end_time = int(time.time() * 1000)
        # total_execution_time = sum(self.results.values())
        total_execution_time = end_time-start_time
        print(f"SQL, Time(ms)")
        for sql, run_time in sorted(self.results.items()):
            print(f"{sql}, {run_time}")
        print(f"Total Time, {total_execution_time}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--thread', '-t', type=int, default=1,
                        help="thread num")
    parser.add_argument('--sql_dir', '-s', type=str,
                        help="tpcds sql dir")
    parser.add_argument('--host', type=str, default="172.31.29.132",
                        help="fe host")
    parser.add_argument('--port', '-p', type=int, default=9030,
                        help="fe port")
    parser.add_argument('--user', '-u', type=str, default="root",
                        help="user name")
    parser.add_argument('--pwd', '-P', type=str, default="",
                        help="password")
    parser.add_argument('--database', '-d', type=str, default="tpcds_1tb",
                        help="database")
    parser.add_argument('--catalog', '-c', type=str, default="default_catalog",
                        help="catalog")
    args = parser.parse_args()
    sr = StarRocksRunSQL(args.host, args.database, args.user, args.port, args.pwd,args.catalog)
    sr.run_in_threads(args.thread, args.sql_dir)


# python3 tpcds.py -t 1 -s /opt/app/starrocks/tpcds-poc-1.0/sql/tpcds/query/tpcds/