

def write_to_cassandra_status_code(_status_df):
    _status_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "log_analytics") \
        .option("table", "status_code") \
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()


def write_to_cassandra_file_type(_file_type_df):
    _file_type_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "log_analytics") \
        .option("table", "file_type") \
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()


def write_to_cassandra_crawler(_crawler_df):
    _crawler_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "log_analytics") \
        .option("table", "crawler_frequency") \
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()


def write_to_cassandra_bot_hit(_bot_hit_df):
    _bot_hit_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "log_analytics") \
        .option("table", "bot_hits") \
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()
