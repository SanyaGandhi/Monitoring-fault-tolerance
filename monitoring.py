from kafka import KafkaConsumer
import json
import threading
import time
from time import sleep 
import logging
import global_file
import sys


##########    VARIABLE DECLARATIONS    ##########
dict={}

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
logging.basicConfig(level=logging.WARNING, filename=logFile, filemode='w', format='%(asctime)s - [%(levelname)s] - %(filename)s:%(funcName)s:%(lineno)d - %(message)s')

##########    THREAD FUNCTION IMPLEMENTATION    ##########

def mongo_update():
    #function for updating mongo entries using dict
    #DO: update the mango db after connecting
    print("trying to update mongo")
    


def isalive():
    while(True) :
        sleep(5)
        mongo_update()
        if(int(sys.argv[1])==global_file.globe):
            dict2={}
            #DO: copy data from mongo db into a dictionary 'dict2'
            currentTime = time.time()
            for k,vals in dict2.items():
                vals=float(vals)
                diff=currentTime-vals
                if diff>=15 and diff<30:
                    logging.error('The subsystem with instance id = {} has been inactive since a long time'.format(k))
                    print('time to notify the platform admin')
                if vals-currentTime>=45:
                    logging.critical('The subsystem with instance id = {} needs to be killed'.format(k))
                    print('time to notify kill the instance')
                    #DO: delete the entry from mongo db
            if int(sys.argv[1])==1:
                global_file.globe=2
            else:
                global_file.globe=1




t = threading.Thread(target = isalive, args=[])
t.start()



##########    MONITORING SUBSYSTEM IMPLEMENTATION    ##########


consumer = KafkaConsumer(kafkaTopicName, group_id=kafkaGroupId, bootstrap_servers=[kafkaIp+":"+kafkaPortNo])

for message in consumer:
    messageContents = message.value.decode('UTF-8').split(':')
    messageContents[0] = messageContents[0][1:]
    messageContents[2] = messageContents[2][:-1]
    key=f'{messageContents[0]}:{messageContents[1]}'
    dict[key]=messageContents[2]
    logging.info('The subsystem = {} with instance id = {} has a new entry'.format(messageContents[0],messageContents[1],messageContents[2]))
    print(dict)

