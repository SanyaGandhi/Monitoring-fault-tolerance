from kafka import KafkaConsumer
import json
import threading
import time
from time import sleep 
import logging


# If the time since the last message received by service is > notifyTime then we'll send a message to the platform Admin via the notification service.
notifyTime = 150

# If the time since the last message received by service is > killTime then we'll pass the service along with its information to the Fault Tolerance system which will deal with it.
killTime = 300
topic_name='monitor_topic'

logging.basicConfig(level=logging.WARNING, filename='monitoring.log', filemode='w', format='%(levelname)s - %(message)s')


def findFault ():
    while(True) :
        # Fetching all the data in activeServices to see if any of them have been inactive for notifyTime or killTime
        jsonFile = open('activeServices.json', 'r')    
        activeServices = json.load(jsonFile)
        jsonFile.close()
        # print('hey!')
        for services in activeServices['activeServices'] : 
            if abs(time.time()-services['timestamp']) > notifyTime :
                logging.error('The subsystem : {} having instance id : {} has been inactive since a long time'.format(services['subsystemName'], services['instanceId']))
                # Use notification service to infrom platform admin that a particular service hasn't responded for a while
            
            if abs(time.time()-services['timestamp']) > killTime :
                logging.critical('The subsystem : {} having instance id : {} has been assumed to have an error'.format(services['subsystemName'], services['instanceId']))
                # Send msg to fault tolerance subsystem that this instance isn't working and need to be dealth with.
            
            # print(time.time())
            # print(services['timestamp'])
            # print(abs(time.time()-services['timestamp']))

        sleep(10)



t = threading.Thread(target = findFault, args=[])
t.start()



consumer = KafkaConsumer(topic_name, group_id='Monitoring', bootstrap_servers=['localhost:9092'])



for message in consumer:
    # messageContents = message.value.decode('UTF-8').split(':')
    messageContents = message.value.decode('UTF-8')
    messageContents=messageContents.split(":")
    
    messageContents[0] = messageContents[0][1:]
    messageContents[2] = messageContents[2][:-1]
    # print(type(messageContents))
    # print(messageContents[0][1:])
    # print(messageContents[1])
    # print(messageContents[2][:-1])

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
        logging.warning('NEW ENTRY : Subsystem : {}, Instance Id : {}, TimeStamp : {}'.format(newService['subsystemName'], newService['instanceId'], newService['timestamp']))
        activeServices['activeServices'].append(newService )

    # Editing json file containing all the data about the activeServices.
    with open("activeServices.json", "w") as write_file :
        json.dump(activeServices, write_file, indent=4)  