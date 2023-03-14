from kafka import KafkaProducer
from time import sleep
import json 
import time

subsystem=input("Enter your subsystem name: ")
id=input("Enter your instance id: ")
topic_name='monitor_topic'
kafka_ip='localhost'
kafka_port='9092'
producer = KafkaProducer(bootstrap_servers=[kafka_ip+":"+kafka_port],api_version=(0, 10, 1))
# conn="connection built"
# producer.send(topic_name, json.dumps(conn).encode('utf-8'))

while True:
    t=time.time()
    message=subsystem+":"+id+":"+str(t)
    producer.send(topic_name, json.dumps(message).encode('utf-8'))
    sleep(60)
    print(message)