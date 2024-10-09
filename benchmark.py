import concurrent.futures
import os
import logging
import random
import threading
import time
from engine.engine import Engine
from util.loguru import logger
import pandas as pd
import numpy as np
from datetime import datetime
from engine.hive.hive_engine import HiveEngine

class RunBenchmark:
    def __init__(self, engine: Engine):
        self.engine = engine
        self.results = {}
        self.run_time = 0

    def get_thread_info(self):
        thread_id = threading.get_ident()
        thread_name = threading.current_thread().name
        return thread_id, thread_name

    def run(self, file_path):
        with open(file_path, 'r') as file:
            sql = file.read()

        thread_id, thread_name = self.get_thread_info()
        logger.info(f"Thread ID: {thread_id}, Thread Name: {thread_name}, Run SQL: {file_path}")
        start_time = int(time.time() * 1000)
        self.engine.execute_sql(sql=sql)
        end_time = int(time.time() * 1000)
        execution_time = end_time - start_time
        sql_name = os.path.basename(file_path).replace(".sql", "")
        if sql_name in self.results:
            self.results[sql_name] = int((self.results[sql_name]+execution_time)/2)
        else:
            self.results[sql_name] = execution_time
        logger.info(f"Thread ID: {thread_id}, Thread Name: {thread_name}, SQL: {file_path} Time: {execution_time} ms")

    def extend_array(self,arr, a):
        if a <= len(arr):
            return arr
        extra_count = a - len(arr)
        # 使用循环补充元素
        extended_array = arr + (arr * (extra_count // len(arr))) + arr[:extra_count % len(arr)]
        return extended_array

    def write_result(self):
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        df = pd.DataFrame(list(sorted(self.results.items())), columns=['sql', 'execution_time(ms)'])
        total = df['execution_time(ms)'].sum()
        mean = df['execution_time(ms)'].mean()
        geometric_mean = np.exp(np.log(df['execution_time(ms)']).mean())
        # 创建统计行
        stats_df = pd.DataFrame([
            {'sql': 'Run Total Time', 'execution_time(ms)': self.run_time},
            {'sql': 'Sum Execution Time', 'execution_time(ms)': total},
            {'sql': 'Arithmetic Mean', 'execution_time(ms)': mean},
            {'sql': 'Geometric Mean', 'execution_time(ms)': geometric_mean}
        ])
        # 合并原始数据和统计数据
        result_df = pd.concat([df, stats_df], ignore_index=True)
        # 使用当前时间创建文件名
        filename = f"result_{current_time}.csv"
        # 将结果写入CSV文件
        result_df.to_csv(filename, index=False)
        logger.info(f"数据已写入 {filename}")
    def run_in_threads(self, thread_count, sql_directory, filter_sql):

        sql_files = [os.path.join(sql_directory, f) for f in os.listdir(sql_directory) if f.endswith('.sql')]
        if filter_sql is not None and filter_sql != '':
            filter_sql_list = filter_sql.split(",")
            run_sql_list = [x for x in sql_files if os.path.basename(x) in filter_sql_list]
        else:
            run_sql_list = sql_files
        random.shuffle(run_sql_list)
        extended_array = self.extend_array(run_sql_list, thread_count)
        random.shuffle(extended_array)
        logging.info(f"SQL: {extended_array}")
        start_time = int(time.time() * 1000)
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            executor.map(self.run, extended_array)
        end_time = int(time.time() * 1000)
        # total_execution_time = sum(self.results.values())
        total_execution_time = end_time-start_time
        logger.info(f"SQL, Time(ms)")
        self.run_time = total_execution_time
        # self.results['Run Total Time'] = total_execution_time
        for sql, run_time in sorted(self.results.items()):
            logger.info(f"{sql}, {run_time}")
        # logger.info(f"Total Time, {total_execution_time}")