from kafka import KafkaProducer
from time import sleep
import json 
import time


def sendheartBeat(subsystemName, subsystemInstanceId) : 

    kafkaIp = "10.2.133.16"
    kafkaPortNo = "9092"
    kafkaTopicName='heartbeatMonitoring'
    
    producer = KafkaProducer(bootstrap_servers=[kafkaIp+":"+kafkaPortNo],api_version=(0, 10, 1))

    while True:
        
        currentTime = time.time()
        message=subsystemName+":"+subsystemInstanceId+":"+str(currentTime)
        
        producer.send(kafkaTopicName, json.dumps(message).encode('utf-8'))
        
        sleep(60)
        