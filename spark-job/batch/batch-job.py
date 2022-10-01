from pyspark.sql import SparkSession
from pyspark.sql import functions as func


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").config("dfs.client.use.datanode.hostname", "true").appName(
        "log-analytics").getOrCreate()

    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .load("hdfs://namenode:8020/data/")

    daily_status = df.select(func.col("status"), func.col("hour"), func.col("month"), func.col("year"),
                                     func.col("crawler"), func.col("date")) \
        .groupby("status", "hour", "month", "year", "date", "crawler").agg(func.count("status").alias("frequency"))

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

    page_crawler = df.withColumn("top_directory", func.regexp_extract("endpoint", "(\/m\/\w+|\/\w+)", 1))

    pages_crawled = page_crawler.select("hour", "month", "year", "date", "crawler", "top_directory").groupBy("hour",
                                                                                                             "month",
                                                                                                             "year",
                                                                                                             "crawler",
                                                                                                             "date",
                                                                                                             "top_directory").agg(
        func.count("top_directory").alias("frequency"))

    pages_crawled = pages_crawled.where(func.col("top_directory") != "")

    bot_hit = df.select("hour", "month", "year", "date", "crawler").groupBy("hour", "month", "year",
                                                                                    "crawler",
                                                                                    "date").agg(func.count("hour")
                                                                                                .alias("frequency"))

