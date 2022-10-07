from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from batch_write import write_to_cassandra_bot_hit, write_to_cassandra_crawler, write_to_cassandra_file_type, write_to_cassandra_status_code
if __name__ == "__main__":
    # spark = SparkSession.builder.master("spark://spark-master:7077").config("dfs.client.use.datanode.hostname", "true").appName(
    #     "log-analytics").getOrCreate()

    spark = SparkSession.builder.master("local[*]").config("dfs.client.use.datanode.hostname", "true").appName(
        "log-analytics").getOrCreate()

    df = spark.read.option("header", "true") \
        .format("parquet") \
        .option("inferSchema", "true") \
        .load("hdfs://namenode:8020/data/log_data/") # hdfs://namenode:8020/data/ ./data_log/

    daily_status = df.select(func.col("status"), func.col("hour"), func.col("month"), func.col("year"),
                             func.col("crawler"), func.col("date")) \
        .groupby("status", "hour", "month", "year", "date", "crawler").agg(func.count("status").alias("frequency"))

    daily_status = daily_status.withColumn("current_timestamp", func.current_timestamp())

    file_type_data = df.withColumn("file_type", func.regexp_extract("endpoint",
                                                                    r"\.(css|jpg|PHP|html|png|gif|jpeg|json|js)",
                                                                    1)).where(func.col("file_type") != "")

    file_type_daily = file_type_data.select(func.col("file_type"), func.col("crawler"), func.col("hour"),
                                            func.col("month"), func.col("year"), func.col("date")).groupBy("file_type",
                                                                                                           "hour",
                                                                                                           "month",
                                                                                                           "year",
                                                                                                           "date",
                                                                                                           "crawler").agg(
        func.count("file_type").alias("frequency"))

    file_type_daily = file_type_daily.withColumn("current_timestamp", func.current_timestamp())

    page_crawler = df.withColumn("top_directory", func.regexp_extract("endpoint", "(\/m\/\w+|\/\w+)", 1))

    pages_crawled = page_crawler.select("hour", "month", "year", "date", "crawler", "top_directory").groupBy("hour",
                                                                                                             "month",
                                                                                                             "year",
                                                                                                             "crawler",
                                                                                                             "date",
                                                                                                             "top_directory").agg(
        func.count("top_directory").alias("frequency"))

    pages_crawled = pages_crawled.where(func.col("top_directory") != "")
    pages_crawled = pages_crawled.withColumn("current_timestamp", func.current_timestamp())

    bot_hit = df.select("hour", "month", "year", "date", "crawler").groupBy("hour", "month", "year",
                                                                            "crawler",
                                                                            "date").agg(func.count("hour")
                                                                                        .alias("frequency"))

    bot_hit = bot_hit.withColumn("current_timestamp", func.current_timestamp())
    # bot_hit.agg(func.sum("frequency")).show()
    # pages_crawled.agg(func.sum("frequency")).show()
    # daily_status.agg(func.sum("frequency")).show()
    # file_type_daily.agg(func.sum("frequency")).show()

    write_to_cassandra_bot_hit(bot_hit)
    write_to_cassandra_crawler(pages_crawled)
    write_to_cassandra_status_code(daily_status)
    write_to_cassandra_file_type(file_type_daily)

# docker exec spark-master /spark/bin/spark-submit  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 --master spark://localhost:7077 opt/spark_store/batch/batch-job.py

# spark-submit  spark-job/batch/batch-job.py
