from pyspark.sql import SparkSession
import findspark

TOPIC = "log_flow"

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("log-analytics").getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()
    spark.stop()

