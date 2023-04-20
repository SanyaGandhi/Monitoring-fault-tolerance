# What does this code do : 

# This code needs to be added within every subsystem so that it may keep sending its heartbeat to the Kafka Stream.

# How to use this code : 

# Step 1 : Add all the necessary imports 
# Step 2 : Create a thread and run this function within the thread. 


from kafka import KafkaProducer
from time import sleep
import json 
import time
import threading


# All variable declaration.
# kafkaIp = "20.106.92.171"
kafkaIp = "0.0.0.0"
kafkaPortNo = "9092"



def sendheartBeat(kafkaTopicName, containerName, nodeId) : 
    
    producer = KafkaProducer(bootstrap_servers=[kafkaIp+":"+kafkaPortNo],api_version=(0, 10, 1))
    
    heartbeat = {
        "container_name" : containerName,
        "node_name" : nodeId,
        "current_time" : time.time()
    }

    # i = 1

    while True:
        try:
            producer.send(kafkaTopicName, json.dumps(heartbeat).encode('utf-8'))
            # print("heartbeatsent",i)
            # i += 1
        except:
            pass

        sleep(60)


# Give the following parameters :

# 1. kafka Topic Name : 

# Available topic Names are : 
# For Monitoring : heartbeat-monitoring
# For Fault Tolerance : heartbeat-fault-tolerance
# For Deployer : heartbeat-deployer
# For Sensor Manager : heartbeat-sensor-manager
# For Load Balancer : heartbeat-load-balancer
# For Node Manager : heartbeat-node-manager
# For Scheduler : heartbeat-scheduler
# For App Controller : heartbeat-validator-workflow
# For Application Developers : heartbeat-developer

# 2. Container Name 

# 3. Node Id



# For testing purposes. Kindly comment out in actual implementation
t1 = threading.Thread(target=sendheartBeat, args=("heartbeat-monitoring", "Container for Monitoring", "1", ))
t2 = threading.Thread(target=sendheartBeat, args=("heartbeat-deployer", "Container for Deployer", "1", ))
t1.start()
t2.start()