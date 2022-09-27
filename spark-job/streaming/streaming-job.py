import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import TimestampType

TOPIC = "log_flow"


def format_logData(raw_df):
    host_exp = r"(^\d{1,3}.\d{1,3}.\d{1,3})"
    time_stamp_exp = r"(\d{1,3}\/\w{1,3}\/\w{1,4}:\d{1,2}:\d{1,2}:\d{1,2} \+\d{1,4})"
    genera_exp = r'\"(\S+)\s(\S+)\s(\S+)"'
    status = r"\s(\d{3})\s"
    content_exp = r'\s(\d+) "'
    useragent_referer_exp = r'"([\S ]*)" [0-9]+ [0-9]+ "([\S ]*)" "([\S |-]*)" '

    data = raw_df.select(func.regexp_extract("value", host_exp, 1).alias("host"),
                         func.regexp_extract("value", time_stamp_exp, 1).alias("timestamp"),
                         func.regexp_extract("value", genera_exp, 1).alias("method"),
                         func.regexp_extract("value", genera_exp, 2).alias("endpoint"),
                         func.regexp_extract("value", genera_exp, 3).alias("protocol"),
                         func.regexp_extract("value", status, 1).cast("int").alias("status"),
                         func.regexp_extract("value", content_exp, 1).cast("int").alias("content_size"),
                         func.regexp_extract("value", useragent_referer_exp, 2).alias("referer"),
                         func.regexp_extract("value", useragent_referer_exp, 3).alias("useragent"))

    return data


def parse_date_and_time(data):
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Jan", "1"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Feb", "2"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Mar", "3"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Apr", "4"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"May", "5"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Jun", "6"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Jul", "7"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Aug", "8"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Sep", "9"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Oct", "10"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Nov", "11"))
    data = data.withColumn("timestamp", func.regexp_replace("timestamp", r"Dec", "12"))

    d2 = data.withColumn("timestamp", parse_datetime(func.col("timestamp")).cast("timestamp"))
    d2 = d2.withColumn("hour", func.hour(func.col("timestamp")))
    d2 = d2.withColumn("day", func.dayofmonth(func.col("timestamp")))
    d2 = d2.withColumn("month", func.month(func.col("timestamp")))
    d2 = d2.withColumn("year", func.year(func.col("timestamp")))
    d2 = d2.withColumn("date", func.to_date(func.col("timestamp")))

    return d2


def select_crawlers(data):
    crawled_df = data.withColumn("crawler", func.regexp_extract("useragent", r"(Mozilla\/5\.0 \(compatible;"
                                                                             r" Googlebot\/2\.1; \+http:\/\/www\.google\."
                                                                             r"com\/bot\.html\)|Safari\/537\.36 "
                                                                             r"\(compatible; Googlebot\/2\.1; \+http:"
                                                                             r"\/\/www\.google\.com\/bot\.html\)$|Mozilla"
                                                                             r"\/5\.0 AppleWebKit\/537\.36 \(KHTML, like "
                                                                             r"Gecko; compatible; Googlebot\/2\.1; \+http:"
                                                                             r"\/\/www\.google\.com\/bot\.html\)|Mozilla"
                                                                             r"\/5\.0 \(compatible; bingbot\/2\.0; \+http"
                                                                             r":\/\/www\.bing\.com\/bingbot\.htm\))",
                                                                1))
    comp_crawler_df = crawled_df.withColumn("crawler", func.when(
        func.col("crawler") == "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Googlebot Desktop").when(
        func.col(
            "crawler") == "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; "
                          "Googlebot/2.1; +http://www.google.com/bot.html)", "Googlebot Desktop")
                                            .when(
        func.col("crawler") == "Safari/537.36 (compatible; Googlebot/2.1; "
                               "+http://www.google.com/bot.html)", "Googlebot Smartphone")
                                            .when(
        func.col("crawler") == "Mozilla/5.0 (compatible; bingbot/2.0; "
                               "+http://www.bing.com/bingbot.htm)", "Bingbot Desktop"))

    comp_crawler_df = comp_crawler_df.where(func.col("crawler") != "")

    return comp_crawler_df


@func.udf()
def parse_datetime(s):
    year = int(s[5:9])
    month = int(s[3])

    day = s[0:2]
    if day[0] == "0":
        day = day[1]

    hour = s[10:12]
    if hour[0] == "0":
        hour = hour[1]

    minute = s[13:15]
    if minute[0] == "0":
        minute = minute[1]

    second = s[16:18]
    if second[0] == "0":
        second = second[1]

    new_date = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))

    return str(new_date)


def write_to_cassandra(comp_df, batch_id):
    comp_df.write\
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "127.0.0.1") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "log_analytics") \
        .option("table", "status_code") \
        .mode("append") \
        .save()


# .option("confirm.truncate", "true")

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").config("dfs.client.use.datanode.hostname", "true").appName(
        "log-analytics").getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # df.writeStream.outputMode()

    parsed_df = format_logData(df)

    parse_df_datetime = parse_date_and_time(parsed_df)

    crawler_df = select_crawlers(parse_df_datetime)

    daily_status = crawler_df.select(func.col("status"), func.col("hour"), func.col("crawler"),
                                     func.col("date")).groupby("status", "hour", "date", "crawler") \
        .agg(func.count("status").alias("count"))

    crawler_df.writeStream \
        .partitionBy("day", "month", "year") \
        .format("parquet") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("append") \
        .start(path="hdfs://namenode:8020/data/")

    daily_status.writeStream \
        .option("checkpointLocation", "tmp/checkpoint") \
        .foreachBatch(write_to_cassandra) \
        .outputMode("update") \
        .start() \
        .awaitTermination()
    # "confirm.truncate"
    # crawler_df.awaitTermination()
    spark.stop()

# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 ./spark-job/streaming/streaming-job.py

# docker exec spark-master /spark/bin/spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 opt/spark_store/streaming/streaming-job.py

# docker exec spark-master /spark/bin/spark-submit  --master spark://localhost:7077 opt/spark_store/batch/batch-job.py
