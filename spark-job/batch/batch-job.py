from pyspark.sql import SparkSession
import findspark

findspark.init()
TOPIC = "log_flow"

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("log-analytics").getOrCreate()
