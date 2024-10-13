### TPCDS多引擎性能测试

#### 1. 数据准备

##### 1.1 设置变量

```shell
# AWS开放的3TB数据集路径
S3_3TB_SOURCE_DATA_PATH=s3://blogpost-sparkoneks-us-east-1/blog/BLOG_TPCDS-TEST-3T-partitioned
# 自己S3 Bucket的路径,存放同步的3TB数据
S3_3TB_DATA_PATH=s3://xxxxx/tpcds-parquet/3tb
```

##### 1.2 AWS开放数据集

* 使用AWS公开3TB数据集不用生成，直接拉到自己s3桶即可, 3TB数据同步10分钟之内(如果桶在us-east-1)
* 数据格式parquet+gzip压缩后总大小923.3G

```shell
# s5cmd 同步会更快
wget https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz
tar -xvzf s5cmd_2.2.2_Linux-64bit.tar.gz
# 如果桶在us-east-1,3tb数据同步时间10分钟之内,如果不在可能需要20分钟以上, 比如从ap-southeast-1同步需要19分钟
echo `date` &&  ./s5cmd cp  $S3_3TB_SOURCE_DATA_PATH/*  $S3_3TB_DATA_PATH/ 2>&1 > logs && echo `date`
# 检查数据大小, 应该是923.3G (parquet+snappy压缩)
./s5cmd du --humanize $S3_3TB_DATA_PATH/*
```

![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202410081201043.png)

#### 2. Hive性能测试

##### 2.1 创建数据库表

* 使用脚本创建表和加载分区
* 24张表，12031分区, 运行时间会比较长,Glue Catalog(1小时左右), Hive Metastore MySQL(16分钟左右)

```shell
python3 -m venv benchmark_venv
source ./benchmark_venv/bin/activate

git clone https://github.com/yhyyz/benchmark-tool.git
cd benchmark-tool
pip3 install -r requirements.txt

nohup python3 create_table.py -i ip-172-31-129-60 -p 10000 -d tmp_tpcds_3tb -l $S3_3TB_DATA_PATH 2>&1 >> /tmp/create-table.log &
# 查看日志
tail -f /tmp/create-table.log
# kill
ps axu|grep create_table.py |gawk 'print $2' |xargs -n1 kill -9 
```

##### 2.2 运行SQL

```shell
HIVE_SERVER2_HOST=xxxx

# run 99 sql
nohup python3 run_hive.py -t 1 --host $HIVE_SERVER2_HOST -p 10000 -d tmp_tpcds_3tb  2>&1 > run_hive.log &
# filter sql,run q1.sql
python3 run_hive.py -t 1 -f q1.sql --host $HIVE_SERVER2_HOST  -p 10000 -d tmp_tpcds_3tb  -s /home/ec2-user/tpcds/benchmark-tool/query/tpcds/hive/
# 指定自定义sql目录
python3 run_hive.py -t 1 -f q1.sql --host $HIVE_SERVER2_HOST -p 10000 -d tmp_tpcds_3tb  -s /home/ec2-user/tpcds/benchmark-tool/query/tpcds/hive/ 

```

#### 3. Spark性能测试

##### 3.1 spark-thriftserver or kyuubi

```
sudo sh /usr/lib/spark/sbin/start-thriftserver.sh  --master yarn --executor-memory 32G  --driver-memory 16G  --executor-cores  4  -- --num-executors 28    --hiveconf hive.server2.thrift.port=10001   --conf spark.dynamicAllocation.enabled=false
```

##### 3.2 运行SQL

```shell
SPARK_HIVE_SERVER2_HOST=xxxx

# run 99 sql
nohup python3 run_spark.py -t 1 --host $SPARK_HIVE_SERVER2_HOST -p 10001 -d tmp_tpcds_3tb  2>&1 > run_spark.log &
# filter sql,run q1.sql
python3 run_spark.py -t 1 -f q1.sql --host $SPARK_HIVE_SERVER2_HOST  -p 10001 -d tmp_tpcds_3tb  -s /home/ec2-user/tpcds/benchmark-tool/query/tpcds/spark/
# 指定自定义sql目录
python3 run_hive.py -t 1 -f q1.sql --host $SPARK_HIVE_SERVER2_HOST -p 10001 -d tmp_tpcds_3tb  -s /home/ec2-user/tpcds/benchmark-tool/query/tpcds/spark/ 
```


