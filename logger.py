from kafka import KafkaConsumer
from time import sleep


# Will remain same
kafkaIp = "10.2.133.16"
kafkaPortNo = "9092"
kafkaTopicName = 'loggingMonitoring' 

# Change based on which subsystem you want to log.

kafkaGroupId = "monitoring"
logFile = "monitoring.log" 



def logging (kafkaIp, kafkaPortNo, kafkaTopicName, kafkaGroupId, logFile) :
    
    logging.basicConfig(level=logging.DEBUG, filename=logFile, filemode='w', format='%(asctime)s - [%(levelname)s] - %(filename)s:%(funcName)s:%(lineno)d - %(message)s')
    
    consumer = KafkaConsumer(kafkaTopicName, group_id=kafkaGroupId, bootstrap_servers=[kafkaIp+":"+kafkaPortNo])
    
    for message in consumer:
        
        messageContents = message.value.decode('UTF-8').split(':')

        # The following two lines are done to get rid of the extra quotes in the first and last elements of the list messageContents
        messageContents[0] = messageContents[0][1:]
        messageContents[1] = messageContents[1][:-1]


        if messageContents[0] == "DEBUG" :
            logging.debug(messageContents[1])
        elif messageContents[0] == "INFO" :
            logging.info(messageContents[1])
        elif messageContents[0] == "WARNING" :
            logging.warning(messageContents[1])
        elif messageContents[0] == "ERROR" :
            logging.error(messageContents[1])
        else : 
            logging.critical(messageContents[1])

    sleep(10)
