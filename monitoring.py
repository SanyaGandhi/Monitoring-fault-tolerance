from kafka import KafkaConsumer
import json
import threading
import time
from time import sleep
import logging
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
notifyTime = 15

# If the time since the last message received by service is > killTime then we'll pass the service along with its information to the Fault Tolerance system which will deal with it.
killTime = 30

# To connect to the kafka stream
# kafkaIp = "20.106.92.171" # IP of external globalValue where kafka is supposed to run.
kafkaIp = "0.0.0.0"
kafkaPortNo = "9092"
kafkaTopicName = "heartbeatMonitoring"
kafkaGroupId = "Monitoring"

# Log should be stored in logFile
logFile = "Monitoring.log"
logging.basicConfig(level=logging.WARNING, filename=logFile, filemode='w',
                    format='%(asctime)s - [%(levelname)s] - %(filename)s:%(funcName)s:%(lineno)d - %(message)s')


##########    THREAD FUNCTION IMPLEMENTATION    ##########

def UpdateGlobalValue(value):
    globalValueCollection = mydb["globalValue"]
    globalValueCollection.update_one({"name": "global_val"}, {"$set": {"value": value}})

    # print(f"Document updated with newvalue = {value} in global")


def GetGlobalValue():
    globalValueCollection = mydb["globalValue"] 
    document = globalValueCollection.find_one({"name": "global_val"})
    return int(document["value"])
    

def mongoUpdate():

    # print("Trying to update Mongo")

    newKafkaDictionary = newDataDictionary 
    
    print(newDataDictionary)
    print(newKafkaDictionary)

    # newDataDictionary.clear()

    for subsystem in newKafkaDictionary:

        # NewDataDictinary contains new messages which have to be updated in mongo
        
        # Define a filter for the subsystem/doc with the specified name
        filter = {"name": subsystem["name"], "containerId": subsystem["containerId"]}#LoadBalancer_2

        # Check whether this subsystem already exists
        existing_subsystem = monitoringCollection.find_one(filter)

        # If the subsystem exists, update its epoctime field
        if existing_subsystem is not None:
            update = {"$set": {"epoc_time": subsystem["epoc_time"]}}
            monitoringCollection.update_one(filter, update)
            print("Document updated.")
        
        # If the document does not exist, insert a new subsystem as new document
        else:
            monitoringCollection.insert_one(subsystem)
            print(f"New subsystem {subsystem['name']} document inserted.")


def isalive():
    while (True):

        sleep(10)

        mongoUpdate()

        if (int(sys.argv[1]) == GetGlobalValue()):
            # ***********************************************
            
            # Notification when heartbeat time exceeds 150 and less than 300
            # Make filter on the basis of time difference
            current_time = time.time()
            filter_for_notification = {"$expr": {"$and": [
                            {"$gt": [{"$subtract": [float(current_time), {"$toDouble": "$epoc_time"}]}, notifyTime]},
                            {"$lt": [{"$subtract": [float(current_time), {"$toDouble": "$epoc_time"}]}, killTime]}
                        ]}}

            # retrieve all subsystems document that matches the filter
            subsystems_to_notify = monitoringCollection.find(filter_for_notification)

            # initialize a list to store the names of documents/ subsystems
            subsystem_names = []

            # iterate over the documents and append names of the documents to the list
            for subsystem in subsystems_to_notify:
                subsystem_names.append(subsystem["name"])
                print("Platform developer have look at them",subsystem["name"],subsystem["containerId"] )

            # print the list of document names
            # for names in subsystem_names:
                # print(f"Documents with name {names['name']} have a time difference between 10 and 20 seconds.")



            # **********************************************************************
            #Iterate and remove
            current_time = time.time()
            
            #make filter on the basis of time exceeding 
            filter_to_kill = {"$expr": {"$gt": [{"$subtract": [float(current_time), {"$toDouble": "$epoc_time"}]}, killTime]}}
            
            # retrieve all subsystems documents that match the filter
            documents_to_remove = monitoringCollection.find(filter_to_kill)

            pprint(documents_to_remove)
            # Delete all documents that match the filter
            result = monitoringCollection.delete_many(filter_to_kill) 

            # Print the number of documents removed
            print(f"Removed {result.deleted_count} documents.")

            new_val = GetGlobalValue()
            new_val = new_val ^ 1
            UpdateGlobalValue(new_val)


t = threading.Thread(target=isalive, args=[])
t.start()


##########    MONITORING SUBSYSTEM IMPLEMENTATION    ##########


consumer = KafkaConsumer(kafkaTopicName, group_id=kafkaGroupId, bootstrap_servers=[f"{kafkaIp}:{kafkaPortNo}"]) 

# print("hi iam running")

for message in consumer:
    
    message_value = message.value.decode('utf-8').strip('"')
    
    print("********",message_value)
    
    name, containerId, VMhost, VMuname, VMpswd, epoc_time = message_value.split(':')
    
    # print()
    # print()
    # problem was newDAtaDictionary appending every time so we are deleteing and end entry 
    # if same name and containerid present
    
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
    #*******************old endtry deleted ****************************************    
        
    #*******************insert new one ********************************************
    subsystem = {
        "name": name,
        "containerId": containerId,
        "VMhost": VMhost,
        "VMuname": VMuname,
        "VMpswd": VMpswd,
        "epoc_time": epoc_time
    }
    newDataDictionary.append(subsystem)

    #******************new entry inserted *****************************************
    # logging.info('The subsystem = {} with instance id = {} has a new entry'.format(
    #     messageContents[0], messageContents[1], messageContents[2]))
    
    print(newDataDictionary)

    