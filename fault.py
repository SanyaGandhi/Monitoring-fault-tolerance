import os

ip='20.193.154.7'
port=567 #dummy port
path='somepath'
vm_name='kafkavm'
conatiner_id=452122 #dummy
commands = ['sshpass -p IASiotplatform@123 ssh iasvm1@20.193.154.7', 'docker stop {conatiner_id}', 'docker start {conatiner_id}', 'exit']


count=0
while commands:   # Checks if the list is not-empty. Loop exits when list is becomes empty
        com = commands.pop(0)
        print ("Start execute commands..")
        os.system(com)
        count += 1
        print (f"[OK] command {str(count)} runing successfully.")

