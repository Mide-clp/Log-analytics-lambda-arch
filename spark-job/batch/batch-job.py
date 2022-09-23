from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.master("local[*]").config("dfs.client.use.datanode.hostname", "true").appName(
    "log-analytics").getOrCreate()

df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://namenode:8020/data/")
df.count()
df.show()
