from benchmark import RunBenchmark
import argparse
from engine.hive.spark_engine import SparkEngine
import os

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(current_dir)

    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--thread', '-t', type=int, default=1,
                        help="thread num, default 1")
    parser.add_argument('--sql_dir', '-s', type=str, default=f"{root_dir}/query/tpcds/spark/",
                        help=f"sql dir,default: {root_dir}/query/tpcds/spark/")
    parser.add_argument('--filter', '-f', type=str,
                        help="only run filtered sql, eg. q1.sql,q2.sql,q5.sql")
    parser.add_argument('--host', type=str, default="localhost",
                        help="host,default localhost")
    parser.add_argument('--port', '-p', type=int, default=10001,
                        help="port,default 10000")
    parser.add_argument('--user', '-u', type=str, default="hadoop",
                        help="user name")
    parser.add_argument('--pwd', '-P', type=str, default="",
                        help="password")
    parser.add_argument('--database', '-d', type=str, default="tpcds_3tb",
                        help="database,default tpcds_3tb")

    args = parser.parse_args()
    hive_engine = SparkEngine(args.host, args.database, args.user, args.port)
    rb = RunBenchmark(hive_engine)
    rb.run_in_threads(args.thread, args.sql_dir, args.filter)
    rb.write_result()
