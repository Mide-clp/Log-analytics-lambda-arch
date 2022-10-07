import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import TimestampType
from stream_utils import select_crawlers, parse_date_and_time, parse_datetime, format_logData
from stream_write import write_to_cassandra_status_code, write_to_cassandra_crawler, write_to_cassandra_bot_hit, \
    write_to_cassandra_file_type

TOPIC = "log_flow"

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("log-analytics").getOrCreate()

    # spark = SparkSession.builder.master("spark://spark-master:7077").config("dfs.client.use.datanode.hostname", "true")\
    #     .config("spark.cores.max", "2") \
    #     .appName(
    #     "log-analytics").getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    # localhost: 9092

    parsed_df = format_logData(df)

    parse_df_datetime = parse_date_and_time(parsed_df)

    crawler_df = select_crawlers(parse_df_datetime)

    crawler_df.writeStream \
        .partitionBy( "year", "month", "day", "hour") \
        .format("parquet") \
        .option("checkpointLocation", "tmp/batch/checkpoint") \
        .outputMode("append") \
        .start(path="hdfs://namenode:8020/data/log_data/")

        # hdfs://namenode:8020/data/log_data/
        # data_log/

    daily_status = crawler_df.select(func.col("status"), func.col("hour"), func.col("month"), func.col("year"),
                                     func.col("crawler"), func.col("date")) \
        .groupby("status", "hour", "month", "year", "date", "crawler").agg(func.count("status").alias("frequency"))
    daily_status = daily_status.withColumn("current_timestamp", func.current_timestamp())

    file_type_data = crawler_df.withColumn("file_type", func.regexp_extract("endpoint",
                                                                            r"\.(css|jpg|PHP|html|png|gif|jpeg|json|js)",
                                                                            1)).where(func.col("file_type") != "")
    file_type_data = file_type_data.withColumn("current_timestamp", func.current_timestamp())
    file_type_daily = file_type_data.select(func.col("file_type"), func.col("crawler"), func.col("hour"),
                                            func.col("month"), func.col("year"), func.col("date")).groupBy("file_type",
                                                                                                           "hour",
                                                                                                           "month",
                                                                                                           "year",
                                                                                                           "date",
                                                                                                           "crawler").agg(
        func.count("file_type").alias("frequency"))
    file_type_daily = file_type_daily.withColumn("current_timestamp", func.current_timestamp())

    page_crawler = crawler_df.withColumn("top_directory", func.regexp_extract("endpoint", r"(\/m\/\w+|\/\w+)", 1))
    pages_crawled = page_crawler.select("hour", "month", "year", "date", "crawler", "top_directory").groupBy("hour",
                                                                                                             "month",
                                                                                                             "year",
                                                                                                             "crawler",
                                                                                                             "date",
                                                                                                             "top_directory").agg(
        func.count("top_directory").alias("frequency"))
    pages_crawled = pages_crawled.where(func.col("top_directory") != "")
    pages_crawled = pages_crawled.withColumn("current_timestamp", func.current_timestamp())

    bot_hit = crawler_df.select("hour", "month", "year", "date", "crawler").groupBy("hour", "month", "year",
                                                                                    "crawler",
                                                                                    "date").agg(func.count("hour")
                                                                                                .alias("frequency"))
    bot_hit = bot_hit.withColumn("current_timestamp", func.current_timestamp())

    bot_hit.writeStream \
        .foreachBatch(write_to_cassandra_bot_hit) \
        .outputMode("update") \
        .start()

    pages_crawled.writeStream \
        .foreachBatch(write_to_cassandra_crawler) \
        .outputMode("update") \
        .start()

    file_type_daily.writeStream \
        .foreachBatch(write_to_cassandra_file_type) \
        .outputMode("update") \
        .start()

    daily_status.writeStream \
        .option("checkpointLocation", "tmp/stream/checkpoint") \
        .foreachBatch(write_to_cassandra_status_code) \
        .outputMode("update") \
        .start()

    spark.streams.awaitAnyTermination()

    spark.stop()

# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 ./spark-job/streaming/streaming-job.py

# docker exec spark-master /spark/bin/spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 opt/spark_store/streaming/streaming-job.py


