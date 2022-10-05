#docker exec namenode hdfs dfs -rm -r /data/
docker exec namenode hdfs dfs -mkdir /data
docker exec namenode hdfs dfs -chmod 777 /data

docker exec cassandra cqlsh --username cassandra --password cassandra  -f /schema.cql