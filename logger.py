# How to run : 

# Command : python3 logger.py <a> <b>
# a = Kafka topic of the subystem you want to log.
# b = Kafka group Id. Should be same for logging the same subsystem.

# Eg : 

# For logging the first istance of monitoring we'll write : 
# python3 logger.py log-Monitoring-1 Monitoring 

# For logging the second istance of monitoring we'll write : 
# python3 logger.py log-Monitoring-2 Monitoring 


# Kafka topics that will be create for each subsystem for logging: 

# For Monitoring : log-Monitoring-1, log-Monitoring-2
# For Fault Tolerance : log-FaultTolerance-1, log-FaultTolerance-2
# For Deployer : log-Deployer-1, log-Deployer-2
# For Sensor Manager : log-SensorManager-1, log-SensorManager-2
# For Load Balancer : log-LoadBalancer-1, log-LoadBalancer-2
# For Node Manager : log-NodeManager-1, log-NodeManager-2
# For Scheduler : log-Scheduler-1, log-Scheduler-2
# For App Controller : log-AppController-1, log-AppController-2



from kafka import KafkaConsumer
from time import sleep
import sys
# import logging
import pymongo
from datetime import datetime

# Will remain same for all subsystems.
# kafkaIp = "10.2.133.16"
kafkaIp = "0.0.0.0"
kafkaPortNo = "9092"
client = pymongo.MongoClient("mongodb+srv://test:test@cluster0.xcykxcz.mongodb.net/?retryWrites=true&w=majority")
mydb = client["IAS_PROJECT"]


# Change based on which subsystem you want to log.
kafkaTopicName = sys.argv[1] 
kafkaGroupId = sys.argv[2]
loggingCollection = mydb[sys.argv[1]]

def logSystem (kafkaTopicName, kafkaGroupId) :
    
    consumer = KafkaConsumer(kafkaTopicName, group_id=kafkaGroupId, bootstrap_servers=[kafkaIp+":"+kafkaPortNo])
    
    for message in consumer:
        
        messageContents = message.value.decode('UTF-8').split(':')

        # The following two lines are done to get rid of the extra quotes in the first and last elements of the list messageContents
        messageContents[0] = messageContents[0][1:]
        messageContents[1] = messageContents[1][:-1]

        dateAndTime = datetime.now()

        log_entry = {
        "date": dateAndTime.strftime("%Y-%m-%d"),
        "time": dateAndTime.strftime("%H:%M:%S"),
        "severity": messageContents[0],
        "message": messageContents[1]
        }

        loggingCollection.insert_one(log_entry)

    sleep(10)

logSystem(kafkaTopicName, kafkaGroupId)