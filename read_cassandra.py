import datetime
import logging
import sys
import time
from datetime import timedelta

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import pandas as pd

try:
    cluster = Cluster(["127.0.0.1"], port="9042")
    session = cluster.connect("log_analytics")
except Exception as e:
    print("Couldn't connect to the cassandra cluster")
    logging.error(e)
    sys.exit(1)


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def get_cassandra_data(stmt):
    session.row_factory = pandas_factory
    session.default_fetch_size = None

    result = session.execute(stmt)

    df = result._current_rows

    return df


def join_real_batch_data(batch_table, realtime_table):
    sql = \
        """
    SELECT * FROM %s;
    """ % batch_table
    data = get_cassandra_data(sql)
    data["date"] = pd.to_datetime(data["date"])
    # print(data)

    try:
        max_hour = max(data["hour"])
        date_stamp = max(data["date"])
        formatted_date_stamp = str(date_stamp)[:22]

    except ValueError as e:
        sql = \
            """
        SELECT * FROM %s;
        """ % realtime_table
        data = get_cassandra_data(sql)

        max_hour = min(data["hour"])
        date_stamp = max(data["date"])
        formatted_date_stamp = str(date_stamp)[:22]


    sql_2 = \
        """
    SELECT * FROM %s WHERE date >= '%s' AND hour >= %s  ALLOW FILTERING;
    """ % (realtime_table, formatted_date_stamp, max_hour)
    non_batch_data = get_cassandra_data(sql_2)

    sql_3 = \
        """
    SELECT * FROM %s WHERE hour < %s  ALLOW FILTERING;
    """ % (batch_table, max_hour)

    batch_data = get_cassandra_data(sql_3)
    batch_data["date"] = pd.to_datetime(batch_data["date"])
    joined_data = pd.concat([non_batch_data, batch_data])

    return joined_data


# print(join_real_batch_data("file_type", "file_type_realtime"))