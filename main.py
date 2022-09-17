from kafka import KafkaProducer
TOPIC = "log_flow"
producer=None
try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
except Exception as e:
    print(e)
msg = bytearray("hello".encode("utf-8"))

for x in range(5):
    producer.send(TOPIC, msg)
    print(msg)


