from kafka import KafkaConsumer
import json
import threading
import time
from time import sleep
import logging
import global_file
import sys
from pprint import pprint


########### for mongodatabase ############################
import pymongo
import json
from pymongo import MongoClient

# Connect to python applicaiton
client = pymongo.MongoClient(
    "mongodb+srv://test:test@cluster0.xcykxcz.mongodb.net/?retryWrites=true&w=majority")
db = client.test
# print(client,db)
db_names = client.list_database_names()
# print(db_names)
# create a new database
mydb = client["IAS_PROJECT"]
# i have created  it explicitly

# Create a new collection called 'Monitoring' in the 'IAS_PROJECT' database
monitoringCollection = mydb["Monitoring"]
##########################################################


##########    VARIABLE DECLARATIONS    ##########
newDataDictionary = {}

# If the time since the last message received by service is > notifyTime then we'll send a message to the platform Admin via the notification service.
notifyTime = 15

# If the time since the last message received by service is > killTime then we'll pass the service along with its information to the Fault Tolerance system which will deal with it.
killTime = 30

# To connect to the kafka stream
kafkaIp = "20.106.92.171"
kafkaPortNo = "9092"
kafkaTopicName = "testing"
kafkaGroupId = "Monitoring"

# Log should be stored in logFile
logFile = "Monitoring.log"
logging.basicConfig(level=logging.WARNING, filename=logFile, filemode='w',
                    format='%(asctime)s - [%(levelname)s] - %(filename)s:%(funcName)s:%(lineno)d - %(message)s')

##########    THREAD FUNCTION IMPLEMENTATION    ##########


def mongoUpdate():

    print("Trying to update Mongo")

    # Define the _id value of the document to check 
    # hardcoded this particular document
    doc_id = "6432c5ccaf09d148f7cd6e1a"

    # Find the document with the specified _id value
    old_values = monitoringCollection.find_one({"_id": doc_id})
    
    # Get colletion from MongoDb
    # old_values = monitoringCollection.find_one()
    
    print('old doc', old_values)
    
    # Iterate over new values and update if they are greater than the old ones
    if old_values is not None:
        for key, value in newDataDictionary.items():
            if  key in old_values and value > old_values[key]:
                monitoringCollection.update_one({'_id': old_values['_id']}, {'$set': {key: value}})

        new_pairs = set(newDataDictionary.items()) - set(old_values.items())
        if new_pairs:
            
            # Add new key-value pairs to old_values
            for key, value in new_pairs:
                old_values[key] = value

            # Insert updated document back into collection          
            monitoringCollection.delete_one({'_id': old_values['_id']})
            
            # Delete the old document
            monitoringCollection.replace_one({'_id': old_values['_id']}, old_values, upsert=True)
    else:
        doc = monitoringCollection.insert_one(newDataDictionary)
        print("Inserted document with ID:", doc.inserted_id)

    print("Updated mongo")


def isalive():
    while (True):
        sleep(10)
        mongoUpdate()
        if (int(sys.argv[1]) == global_file.globe):
            # ***********************************************
            # DO: copy data from mongo db into a newDataDictionaryionary 'allDbData'
            allDbData = monitoringCollection.find_one()
            print(allDbData)
            currentTime = time.time()
            for k, vals in allDbData.items():
                # _id also created when we first time update
                '''Performing an update on the path '_id'
                  would modify the immutable field '_id', 
                  full error: {'index': 0, 'code': 66, 'errmsg':
                    "Performing an update on the path '_id'
                      would modify the immutable field '_id'"}
                '''
                print('******************************************************')
                if k != '_id':  # for ignoring above error
                    vals = float(vals)
                    diff = currentTime-vals
                    print(diff)
                    if diff >= notifyTime and diff < killTime:
                        logging.error(
                            'The subsystem with instance id = {} has been inactive since a long time'.format(k))
                        print('time to notify the platform admin')
                    if diff >= killTime:
                        logging.critical(
                            'The subsystem with instance id = {} needs to be killed'.format(k))
                        print('time to notify & kill the instance')
                        # DO: delete the entry from mongo db
                        doc_id=allDbData['_id']
                        monitoringCollection.update_one({"_id": doc_id}, {
                                         "$unset": {k: ""}})
    
            if int(sys.argv[1]) == 1:
                global_file.globe = 2
            else:
                global_file.globe = 1


t = threading.Thread(target=isalive, args=[])
t.start()


##########    MONITORING SUBSYSTEM IMPLEMENTATION    ##########


consumer = KafkaConsumer(kafkaTopicName, group_id=kafkaGroupId,
                         bootstrap_servers=[kafkaIp+":"+kafkaPortNo])

for message in consumer:
    messageContents = message.value.decode('UTF-8').split(':')
    messageContents[0] = messageContents[0][1:]
    messageContents[2] = messageContents[2][:-1]
    key = f'{messageContents[0]}:{messageContents[1]}'
    newDataDictionary[key] = messageContents[2]
    logging.info('The subsystem = {} with instance id = {} has a new entry'.format(
        messageContents[0], messageContents[1], messageContents[2]))
    print(newDataDictionary)

    # my assumption is you are providing a code such as
    # SubsystemName_SubsytemId as key with splitter is underscore'_'
