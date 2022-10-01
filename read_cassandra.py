import logging
import sys
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


sql = "SELECT * FROM status_code"


def get_cassandra_data(stmt):
    session.row_factory = pandas_factory
    session.default_fetch_size = None

    result = session.execute(stmt)

    df = result._current_rows

    return df


# data = get_cassandra_data(sql)

