# How to run : 

# Command : python3 monitoring.py <a> <b>
# a = Subsystem instance we wish to run. 
# b = Logging topic name on which we wish to log 

# Eg : 

# For logging the first istance of monitoring we'll write : 
# python3 monitoring.py 1 log-Monitoring-1 

# For logging the second istance of monitoring we'll write : 
# python3 monitoring.py 2 log-Monitoring-2


from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import threading
import time
from time import sleep
import sys
from pprint import pprint


########### For Mongo Database ############################

import pymongo
import json

# Connect to python applicaiton
client = pymongo.MongoClient("mongodb+srv://test:test@cluster0.xcykxcz.mongodb.net/?retryWrites=true&w=majority")

# Creating a new database
mydb = client["IAS_PROJECT"]

# Create a new collection called 'Monitoring' in the 'IAS_PROJECT' database
monitoringCollection = mydb["Monitoring"]

##########################################################


##########    VARIABLE DECLARATIONS    ##########
newDataDictionary = []

# If the time since the last message received by service is > notifyTime then we'll send a message to the platform Admin via the notification service.
notifyTime = 30

# If the time since the last message received by service is > killTime then we'll pass the service along with its information to the Fault Tolerance system which will deal with it.
killTime = 60

# To connect to the kafka stream
# kafkaIp = "20.106.92.171" # IP of external globalValue where kafka is supposed to run.
kafkaIp = "0.0.0.0"
kafkaPortNo = "9092"
kafkaTopicName = "heartbeatMonitoring"
kafkaGroupId = "Monitoring"


# Setting up logging credentials : 
loggingTopicName = sys.argv[2]
producerForLogging = KafkaProducer(bootstrap_servers=[kafkaIp+":"+kafkaPortNo],api_version=(0, 10, 1))


##########    THREAD FUNCTION IMPLEMENTATION    ##########

def UpdateGlobalValue(value):

    global_val = {
        "name" : "global_val",
        "value" : value
    }

    globalValueCollection = mydb["globalValue"]

    document = globalValueCollection.find_one({"name": "global_val"})
    if document is None:
        globalValueCollection.insert_one(global_val)
    else :
        globalValueCollection.update_one({"name": "global_val"}, {"$set": {"value": value}})
       


def GetGlobalValue():
    globalValueCollection = mydb["globalValue"] 
    document = globalValueCollection.find_one({"name": "global_val"})
    return int(document["value"])
    
    

def mongoUpdate():
    
    # print(newDataDictionary)
    # print(newKafkaDictionary)

    for subsystem in newDataDictionary.copy():

        # NewDataDictinary contains new messages which have to be updated in mongo
        
        # Define a filter for the subsystem/doc with the specified name
        filter = {"name": subsystem["name"], "containerId": subsystem["containerId"]}

        # Check whether this subsystem already exists
        existing_subsystem = monitoringCollection.find_one(filter)

        # If the subsystem exists, update its epoctime field
        if existing_subsystem is not None:
            update = {"$set": {"epoc_time": subsystem["epoc_time"]}}
            monitoringCollection.update_one(filter, update)        

        # If the document does not exist, insert a new subsystem as new document if its time stamp is less than kill time.
        else:
            if float(time.time()) - float(subsystem["epoc_time"]) < killTime : 
                monitoringCollection.insert_one(subsystem)
            else : 
                newDataDictionary.remove(subsystem)
    


def isalive():
    while (True):

        sleep(20)

        mongoUpdate()

        if (int(sys.argv[1]) == GetGlobalValue()):
            
            # Notification when heartbeat time exceeds 150 and less than 300
            # Made filter on the basis of time difference
            current_time = time.time()
            filter_for_notification = {"$expr": {"$and": [
                {"$gt": [{"$subtract": [float(current_time), {"$toDouble": "$epoc_time"}]}, notifyTime]},
                {"$lt": [{"$subtract": [float(current_time), {"$toDouble": "$epoc_time"}]}, killTime]}
            ]}}

            # Retrieve all subsystems document that matches the filter
            subsystems_to_notify = monitoringCollection.find(filter_for_notification)

            # Initialize a list to store the names of documents/ subsystems
            subsystem_names = []

            # Iterate over the documents and append names of the documents to the list
            for subsystem in subsystems_to_notify:
                subsystem_names.append(subsystem["name"])

                message = "WARNING:"+"The susbsystem - {}, having container id - {}, hasn't responded in some time.".format(subsystem["name"], subsystem["containerId"])
                producerForLogging.send(loggingTopicName, json.dumps(message).encode('utf-8'))

                # print("Platform developer have look at them",subsystem["name"],subsystem["containerId"] )


            # Iterate and remove
            current_time = time.time()
            
            # Make filter on the basis of time exceeding 
            filter_to_kill = {"$expr": {"$gt": [{"$subtract": [float(current_time), {"$toDouble": "$epoc_time"}]}, killTime]}}
            
            # Retrieve all subsystems documents that match the filter
            documents_to_remove = monitoringCollection.find(filter_to_kill)


            # Iterating over the documents that will be send to the fault tolerance system to be deleted. 
            for subsystem in documents_to_remove:

                message = "CRITICAL:" + "The susbsystem - {}, having container id - {}, has be scheduled to be deleted.".format(subsystem["name"], subsystem["containerId"])
                producerForLogging.send(loggingTopicName, json.dumps(message).encode('utf-8'))
                
                # TODO : Send all these subsystems to the Fault Tolerance subsystem to kill them. 


            # pprint(documents_to_remove)
            # Delete all documents that match the filter
            result = monitoringCollection.delete_many(filter_to_kill)

            # # Print the number of documents removed
            # print(f"Removed {result.deleted_count} documents.")


            new_val = GetGlobalValue()
            new_val = new_val ^ 3
            UpdateGlobalValue(new_val)


t = threading.Thread(target=isalive, args=[])
t.start()


##########    MONITORING SUBSYSTEM IMPLEMENTATION    ##########


consumer = KafkaConsumer(kafkaTopicName, group_id=kafkaGroupId, bootstrap_servers=[f"{kafkaIp}:{kafkaPortNo}"]) 

for message in consumer:
    
    message_value = message.value.decode('utf-8').strip('"')
    
    name, containerId, VMhost, VMuname, VMpswd, epoc_time = message_value.split(':')
    
    # Problem was newDataDictionary appending every time so we are deleteing and end entry 
    # If same name and containerid present
    
    # ******************* Deleting Old Entry ***********************************
    
    # Check if subsystem with same name and containerId already exists in newDataDictionary
    subsystem_index = None
    for index, subsystem in enumerate(newDataDictionary):
        if subsystem['name'] == name and subsystem['containerId'] == containerId:
            subsystem_index = index
            break

    # If subsystem exists, remove it from newDataDictionary, so avoid duplicacy
    if subsystem_index is not None:
        del newDataDictionary[subsystem_index]
   
        
    #******************* Insert New One ********************************************

    subsystem = {
        "name": name,
        "containerId": containerId,
        "VMhost": VMhost,
        "VMuname": VMuname,
        "VMpswd": VMpswd,
        "epoc_time": epoc_time
    }
    newDataDictionary.append(subsystem)

    