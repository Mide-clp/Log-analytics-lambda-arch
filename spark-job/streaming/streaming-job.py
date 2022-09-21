import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

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


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("log-analytics").getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", TOPIC) \
        .option("checkpointLocation", "checkpoint") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = format_logData(df)

    parsed_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()
    spark.stop()
