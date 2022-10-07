

def write_to_cassandra_status_code(_status_df, _batch_id):
    _status_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "log_analytics") \
        .option("table", "status_code_realtime") \
        .mode("append") \
        .save()


def write_to_cassandra_file_type(_file_type_df, _batch_id):
    _file_type_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "log_analytics") \
        .option("table", "file_type_realtime") \
        .mode("append") \
        .save()


def write_to_cassandra_crawler(_crawler_df, _batch_id):
    _crawler_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "log_analytics") \
        .option("table", "crawler_frequency_realtime") \
        .mode("append") \
        .save()


def write_to_cassandra_bot_hit(_bot_hit_df, _batch_id):
    _bot_hit_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "log_analytics") \
        .option("table", "bot_hits_realtime") \
        .mode("append") \
        .save()
