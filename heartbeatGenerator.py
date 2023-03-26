# What does this code do : 

# This code needs to be added within every subsystem so that it may keep sending its heartbeat to the Kafka Stream.

# How to use this code : 

# Step 1 : Add all the necessary imports 
# Step 2 : Create a thread and run this function within the thread. 



from kafka import KafkaProducer
from time import sleep
import json 
import time

def sendheartBeat(subsystemName, subsystemInstanceId) : 

    kafkaIp = "localhost"
    kafkaPortNo = "9092"
    kafkaTopicName='heartbeatMonitoring'
    
    producer = KafkaProducer(bootstrap_servers=[kafkaIp+":"+kafkaPortNo],api_version=(0, 10, 1))

    while True:
        
        currentTime = time.time()
        message=subsystemName+":"+subsystemInstanceId+":"+str(currentTime)
        
        producer.send(kafkaTopicName, json.dumps(message).encode('utf-8'))
        
        sleep(30)

#for testing purposes
sendheartBeat('sam_pat', '600') #comment out in actual implementation