import kafka.errors
from kafka import KafkaConsumer
import logging

TOPIC = "log_flow"
consumer = None

try:
    consumer = KafkaConsumer(TOPIC, bootstrap_servers="localhost:9092",)

except kafka.errors.KafkaConfigurationError as e:
    logging.error( f"could not connect \n {e} ")

else:
    print("connected successfully")


for msg in consumer:
    print(msg)
    print(msg.value)