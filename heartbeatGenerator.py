# What does this code do : 

# This code needs to be added within every subsystem so that it may keep sending its heartbeat to the Kafka Stream.

# How to use this code : 

# Step 1 : Add all the necessary imports 
# Step 2 : Create a thread and run this function within the thread. 


from kafka import KafkaProducer
from time import sleep
import json 
import time

def sendheartBeat(subsystemName, containerId, VMhost, VMuname, VMpswd) : 

    # kafkaIp = "20.106.92.171"
    kafkaIp = "0.0.0.0"
    kafkaPortNo = "9092"
    kafkaTopicName='heartbeatMonitoring'
    
    producer = KafkaProducer(bootstrap_servers=[kafkaIp+":"+kafkaPortNo],api_version=(0, 10, 1))
    
    i = 1

    while True:
        
        currentTime = time.time()
        message=str(subsystemName)+":"+str(containerId)+":"+str(VMhost)+":"+str(VMuname)+":"+str(VMpswd)+":"+str(currentTime)
        # print(message)
        producer.send(kafkaTopicName, json.dumps(message).encode('utf-8'))
        
        print("heartbeatsent",i)
        i += 1

        sleep(10)

#for testing purposes
sendheartBeat('beta', '600') #comment out in actual implementation.....send random 2 parameters until docker thing is complete

# For testing purposes
sendheartBeat("Paneer", "1",'localhost', 'uname', 'pswd') #comment out in actual implementation