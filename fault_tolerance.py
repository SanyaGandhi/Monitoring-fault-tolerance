import os


#VM details needed from the caller:
#1. vm password
#2. vm name
#3. vm ip
#4. Container id/name needed

#node_id is the key linked to the
def reinitiate_container(container_name, node_id):
    vm_name=node_id #this needs to be extracted from the database that stores the names, ips and passwords of all the vms/nodes
    vm_ip='20.193.154.7' #this again needs to be extracted from the same db
    vm_pswd='IASiotplatform@123' #same as above
    commands = list()
    commands.append(f"docker stop {container_name}")
    commands.append(f"docker start {container_name}")
    commands.append("docker ps -a")
    commands.append("exit")
    command = ';'.join(commands)

    os.system(f"sshpass -p {vm_pswd} ssh {vm_name}@{vm_ip} '" + command + "'")

reinitiate_container('1301ce5c4730','iasvm1') #just for testing purposes.. The specified container id made to run
