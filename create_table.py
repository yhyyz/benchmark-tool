from pyhive import hive
from pyhive.hive import Cursor,Connection
import argparse
from util.timing import timing_decorator
from util.loguru import logger

def get_hive_conn(host, port):
    logger.info("create conn ...")
    conn = hive.connect(host=host, port=port)
    return conn


def read_sql_file(file_path, s3_table_location_prefix):
    with open(file_path, 'r') as file:
        content = file.read()
        content = content.replace("S3_DATA_LOCATION_PREFIX", s3_table_location_prefix)
        # 使用分号split，并移除空白行
        sql_statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
    return sql_statements


@timing_decorator
def run_create_table_sql(conn: Connection, sql_statements, database):
    try:
        cursor = conn.cursor()
        crete_db = f"create database if not exists {database}"
        logger.info(f"create database {database}")
        cursor.execute(crete_db)
        logger.info(f"use database {database}")
        cursor.execute(f"use {database}")
        for statement in sql_statements:
            logger.info(f"exec sql: {statement}")
            cursor.execute(statement)
    except Exception as e:
        logger.error(f"Error occurred: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="create hive/spark table tpcds")
    parser.add_argument('-i', '--host', type=str, default="localhost",
                        help="hive/kyuubi host, default localhost")
    parser.add_argument('-p', '--port', type=int, default="10000",
                        help="hive/kyuubi port, default 10000")
    parser.add_argument('-d', '--database', type=str, default="tmp",
                        help="database name, default tmp")
    parser.add_argument('-l', '--s3_table_location_prefix', '-l', required=True, type=str,
                        help="table location,eg: s3://xxxx/tpcds-parquet/3tb")
    parser.add_argument('-c', '--create_table_ddl', required=False, type=str,
                        default="./ddl/tpcds-3tb-parquet-partitioned.sql",
                        help="table ddl path, default ./ddl/tpcds-3tb-parquet-partitioned.sql")

    args = parser.parse_args()
    host = args.host
    port = args.port
    database = args.database
    s3_table_location_prefix = args.s3_table_location_prefix
    ddl_file = args.create_table_ddl
    sql_statements = read_sql_file(ddl_file, s3_table_location_prefix)
    conn = get_hive_conn(host, port)
    run_create_table_sql(conn, sql_statements, database=database)
    conn.close()
    logger.info("done!")
