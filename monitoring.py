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

# create a new collection called 'Monitoring' in the 'IAS_PROJECT' database
mycol = mydb["Monitoring"]
##########################################################


##########    VARIABLE DECLARATIONS    ##########
dict = {}

# If the time since the last message received by service is > notifyTime then we'll send a message to the platform Admin via the notification service.
notifyTime = 150

# If the time since the last message received by service is > killTime then we'll pass the service along with its information to the Fault Tolerance system which will deal with it.
killTime = 300

# To connect to the kafka stream
kafkaIp = "localhost"
kafkaPortNo = "9092"
kafkaTopicName = "heartbeatMonitoring"
kafkaGroupId = "Monitoring"

# Log should be stored in logFile
logFile = "Monitoring.log"
logging.basicConfig(level=logging.WARNING, filename=logFile, filemode='w',
                    format='%(asctime)s - [%(levelname)s] - %(filename)s:%(funcName)s:%(lineno)d - %(message)s')

##########    THREAD FUNCTION IMPLEMENTATION    ##########


def mongo_update():
    print("trying to update mongo")

    # get collection from mongo db
    old_doc = mycol.find_one()
    print('old doc', old_doc)
    # Iterate over new values and update if they are greater than the old ones
    for key, value in dict.items():
        if key in old_doc and value > old_doc[key]:
            mycol.update_one({'_id': old_doc['_id']}, {'$set': {key: value}})
        else:
            old_doc[key] = value
            # x = mycol.insert_one(result)

            # print the object ID of the inserted document
            # print(x.inserted_id)

    # Check if there are any new key-value pairs in result that were not in old_values
    new_pairs = set(dict.items()) - set(old_doc.items())
    if new_pairs:
        # Add new key-value pairs to old_doc
        for key, value in new_pairs:
            old_doc[key] = value
        # Insert updated document back into collection
        mycol.replace_one({'_id': old_doc['_id']}, old_doc, upsert=True)
    # time.sleep(5)
    print("Updated mongo")


def isalive():
    while (True):
        sleep(5)
        mongo_update()
        if (int(sys.argv[1]) == global_file.globe):
            # ***********************************************
            # DO: copy data from mongo db into a dictionary 'dict2'
            dict2 = mycol.find_one()
            currentTime = time.time()
            print('dicti-onary- 2-------------------',dict2)
            print('I am here')
            for k, vals in dict2.items():
                # _id also created when we first time update
                '''Performing an update on the path '_id'
                  would modify the immutable field '_id', 
                  full error: {'index': 0, 'code': 66, 'errmsg':
                    "Performing an update on the path '_id'
                      would modify the immutable field '_id'"}
                '''
                if k != '_id':  # for ignoring above error
                    vals = float(vals)
                    diff = currentTime-vals
                    if diff >= 15 and diff < 30:
                        logging.error(
                            'The subsystem with instance id = {} has been inactive since a long time'.format(k))
                        print('time to notify the platform admin')
                    if vals-currentTime >= 45:
                        logging.critical(
                            'The subsystem with instance id = {} needs to be killed'.format(k))
                        print('time to notify kill the instance')
                        # DO: delete the entry from mongo db
                        mycol.update_one({"_id": dict2["_id"]}, {
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
    dict[key] = messageContents[2]
    logging.info('The subsystem = {} with instance id = {} has a new entry'.format(
        messageContents[0], messageContents[1], messageContents[2]))
    print(dict)

    # my assumption is you are providing a code such as
    # SubsystemName_SubsytemId as key with splitter is underscore'_'
