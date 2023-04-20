# How to run : 

# Command : python3 monitoring.py

# Eg : 

# For logging the first istance of monitoring we'll write : 
# python3 monitoring.py 


from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import threading
import time
from time import sleep
from kafka.errors import KafkaError
import random
from loggingMessage import *



########### For Mongo Database ############################

import pymongo
import json

# Connect to mongodb server
client = pymongo.MongoClient("mongodb+srv://test:test@cluster0.xcykxcz.mongodb.net/?retryWrites=true&w=majority")

# Creating a new database
mydb = client["IAS_PROJECT"]

# Create a new collection called "standardMonitoring" and "alertedMonitoring" in the 'IAS_PROJECT' database
standardMonitoringCollection = mydb["standardMonitoring"]
alertedMonitoringCollection = mydb["alertedMonitoring"]



##########    VARIABLE DECLARATIONS    ##########

# If the time since the last message received by service is > notifyTime then we'll send a message to the platform Admin via the notification service.
notifyTime = 250

# If the time since the last message received by service is > killTime then we'll pass the service along with its information to the Fault Tolerance system which will deal with it.
killTime = 300

# To connect to the kafka stream
# kafkaIp = "20.106.92.171" # IP of external globalValue where kafka is supposed to run.
kafkaIp = "0.0.0.0"
kafkaPortNo = "9092"
kafkaTopicName = ["heartbeat-monitoring", "heartbeat-fault-tolerance", "heartbeat-deployer", "heartbeat-sensor-manager", "heartbeat-load-balancer", "heartbeat-node-manager", "heartbeat-scheduler", "heartbeat-validator-workflow", "heartbeat-developer"]
kafkaGroupId = "Monitoring"

# Setting up logging credentials : 
producerForLogging = KafkaProducer(bootstrap_servers=[kafkaIp+":"+kafkaPortNo],api_version=(0, 10, 1))



##########    THREAD FUNCTION IMPLEMENTATION    ##########

# Function for monitoring the containers that haven't send their heartbeat for more than 250 seconds. In case these systems don't sent a heartbeat for 500 seconds then they are to be declared dead and their container has to be restarted.

def alertedMonitoring():

    while (True):

        # Made filter on the basis of time difference
        current_time = time.time()
        filter_to_kill = {"$expr": {"$gt": [{"$subtract": [float(current_time), {"$toDouble": "$current_time"}]}, killTime]}}
        
        # Retrieve all subsystems documents that match the filter
        documents_to_remove = alertedMonitoringCollection.find(filter_to_kill)

        # Iterating over the documents that will be send to the fault tolerance system to be deleted. 
        for subsystem in documents_to_remove:
            log_message("monitoring", "CRITICAL", "The susbsystem having container name - {} and node name - {} has be scheduled to be deleted.".format(subsystem["container_name"], subsystem["node_name"]))
            alertedMonitoringCollection.delete_one(subsystem)
            
            # TODO : Send all these subsystems to the Fault Tolerance subsystem to kill them. 

        sleep(30)

        

# Function for monitoring the containers that haven't send their heartbeat for more than 250 seconds. In case these systems don't sent a heartbeat for 250 seconds then they are to be shifted with the alerted monitoring db and a notification has to be sent to the platform admin that this particular subsystem isn't acting well. 

def standardMonitoring():

    sleep(random.randint(0, 60))

    # print("Will finally enter the standard Monitoring code")

    while (True):
            
        # Notification when heartbeat time exceeds 150 and less than 300
        
        current_time = time.time()
        filter_for_notification = {"$expr": {"$gt": [{"$subtract": [float(current_time), {"$toDouble": "$current_time"}]}, notifyTime]}}

        # Retrieve all subsystems document that matches the filter
        subsystems_to_notify = standardMonitoringCollection.find(filter_for_notification)

        # Initialize a list to store the container_name of documents/ subsystems
        subsystem_names = []

        # Iterate over the documents and append container_name of the documents to the list
        for subsystem in subsystems_to_notify:
            subsystem_names.append(subsystem["container_name"])
            log_message("monitoring", "WARNING", "The susbsystem having container name - {} and node name - {} hasn't responded in some time.".format(subsystem["container_name"], subsystem["node_name"]))
            alertedMonitoringCollection.insert_one(subsystem)
            standardMonitoringCollection.delete_one(subsystem)

            # TODO : Send message to the platform admin telling him that this particular node hasn't responded in quiet some time.
        
        sleep(60)




t1 = threading.Thread(target=standardMonitoring, args=[])
t2 = threading.Thread(target=alertedMonitoring, args=[])
t1.start()
t2.start()



##########    MONITORING SUBSYSTEM IMPLEMENTATION    ##########

def startMonitoring(kafkaTopicName): 

    consumer = KafkaConsumer(kafkaTopicName, group_id=kafkaGroupId, bootstrap_servers=[f"{kafkaIp}:{kafkaPortNo}"]) 

    try:

        for message in consumer:
        
            messageContents = json.loads(message.value)

            # print(messageContents)
            
            # Define a filter for the subsystem/doc with the specified name
            filter = {"_id": messageContents["container_name"]} # Takes O(log n)

            # Check whether this subsystem already exists
            existing_subsystem = standardMonitoringCollection.find_one(filter)

            # If the subsystem exists, update its current_time field
            if existing_subsystem is not None:
                update = {"$set": {"current_time": messageContents["current_time"]}}
                standardMonitoringCollection.update_one(filter, update)        

            # If the document does not exist, insert a new subsystem as new document if its time stamp is less than kill time.
            else : 
                messageContents["_id"] = messageContents["container_name"]
                standardMonitoringCollection.insert_one(messageContents)
            
        
    except KeyboardInterrupt:
        pass

    except KafkaError as kError:
        print(f'Error while consuming messages from topics {kafkaTopicName}: {kError}')


for topic in kafkaTopicName :
    t = threading.Thread(target=startMonitoring, args=(topic, ))
    t.start()