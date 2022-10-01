from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.master("local[*]").config("dfs.client.use.datanode.hostname", "true").appName(
    "log-analytics").getOrCreate()

df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://namenode:8020/data/")
df.count()
df.show()



import datetime

d = datetime.datetime.

def ArrayChallenge(arr):
    buildings = len(arr)
    l_buildings = []
    r_buildings = []

    water = 0

    current_max_building = 0
    for x in range(buildings):
        if arr[x] > current_max_building:
            current_max_building = arr[x]

        l_buildings.append(current_max_building)
    new_max_building = 0

    for x in range(buildings + 1):
        if arr[-x] > new_max_building:
            new_max_building = arr[-x]
        r_buildings.insert(0, current_max_building)

    for i, building in enumerate(arr):
        water += min(l_buildings[i], r_buildings[i]) - building

    return water


print(ArrayChallenge(input()))

# https://stackoverflow.com/questions/24414700/water-collected-between-towers?rq=1
