from kafka import KafkaConsumer
import json
import threading
import time
from time import sleep 
import logging


##########    VARIABLE DECLARATIONS    ##########


# If the time since the last message received by service is > notifyTime then we'll send a message to the platform Admin via the notification service.
notifyTime = 150

# If the time since the last message received by service is > killTime then we'll pass the service along with its information to the Fault Tolerance system which will deal with it.
killTime = 300

# To connect to the kafka stream
kafkaIp = "10.2.133.16"
kafkaPortNo = "9092"
kafkaTopicName = "heartbeatMonitoring"
kafkaGroupId = "Monitoring"

# Log should be stored in logFile
logFile = "Monitoring.log"
logging.basicConfig(level=logging.WARNING, filename=logFile, filemode='w', format='%(asctime)s - [%(levelname)s] - %(filename)s:%(funcName)s:%(lineno)d - %(message)s')

# JsonFile for currently active Services
currentActiveServices = "activeServices.json"


##########    THREAD FUNCTION IMPLEMENTATION    ##########


# Bad Code : Use custom made heap O(logn) or use One thread per monitoring entity. 

def findFault ():

    while(True) :
        
        # Fetching all the data from activeServices to see if any of them have been inactive uptill notifyTime or killTime
        jsonFile = open(currentActiveServices, 'r')    
        activeServices = json.load(jsonFile)
        jsonFile.close()

        for services in activeServices['activeServices'] : 
            if abs(time.time()-services['timestamp']) > notifyTime :
                logging.error('The subsystem : {} having instance id : {} has been inactive since a long time'.format(services['subsystemName'], services['instanceId']))
                # Use notification service to infrom platform admin that a particular service hasn't responded for a while
            
            if abs(time.time()-services['timestamp']) > killTime :
                logging.critical('The subsystem : {} having instance id : {} has been assumed to have an error'.format(services['subsystemName'], services['instanceId']))
                # Send msg to fault tolerance subsystem that this instance isn't working and need to be dealth with.
            
            print(time.time())
            # print(services['timestamp'])
            # print(abs(time.time()-services['timestamp']))

        sleep(60)



t = threading.Thread(target = findFault, args=[])
t.start()



##########    MONITORING SUBSYSTEM IMPLEMENTATION    ##########


consumer = KafkaConsumer(kafkaTopicName, group_id=kafkaGroupId, bootstrap_servers=[kafkaIp+":"+kafkaPortNo])

for message in consumer:
    messageContents = message.value.decode('UTF-8').split(':')

    # Checking the current activeServices file to monitor all the currently active services. 
    jsonFile = open('activeServices.json', 'r')    
    activeServices = json.load(jsonFile)
    jsonFile.close()


    flag = False # To check if the current service is present in our activerServices.json file.


    # Updating old timestamp if service is present.
    for services in activeServices['activeServices'] : 
        if messageContents[0] == services['subsystemName'] and messageContents[1] == services['instanceId']:
            flag = True
            if services['timestamp'] < messageContents[2] :
                services['timestamp'] = messageContents[2]
                logging.warning('UPDATE : Subsystem : {}, Instance Id : {}, TimeStamp : {}'.format(services['subsystemName'], services['instanceId'], services['timestamp']))
            break
        
    # Incase service is not present, we add its data to our dictionary of activeServices. 
    if flag == False : 
        newService = { "subsystemName" : messageContents[0], "instanceId" : int(messageContents[1]), "timestamp" : float(messageContents[2])}
        activeServices['activeServices'].append(newService)
        logging.warning('NEW ENTRY : Subsystem : {}, Instance Id : {}, TimeStamp : {}'.format(newService['subsystemName'], newService['instanceId'], newService['timestamp']))
        

    # Editing json file containing all the data about the activeServices.
    with open("activeServices.json", "w") as write_file :
        json.dump(activeServices, write_file, indent=4)  