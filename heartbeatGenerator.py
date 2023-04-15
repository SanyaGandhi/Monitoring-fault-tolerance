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

# Give the following parameters :
# 1. Subsystem Name 
# 2. Container Id
# 3. VM Hostname 
# 4. VM username 
# 5. VM password

# For testing purposes
sendheartBeat("Paneer", "1",'localhost', 'uname', 'pswd') #comment out in actual implementation