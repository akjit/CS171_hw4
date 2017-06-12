import os
import sys
import socket
import subprocess
from log import log
from cli import CLI
from prm import PRM
from map import Mapper
from reducer import Reducer

def main():

    if len(sys.argv) == 2:
        nodeId = sys.argv[1]
    else:
        print("Please enter an id for the node")
        exit(1)

    my_ip = "127.0.0.1"
    
    if int(nodeId) == 1:
        ip_1 = "10.2.24.77"
        ip_2 = "10.2.24.73"
    elif int(nodeId) == 2:
        ip_1 = "10.2.24.88"
        ip_2 = "10.2.24.73"
    elif int(nodeId) == 3:
        ip_1 = "10.2.24.88"
        ip_2 = "10.2.24.77"
    else:
        print("Improper node id. Choose between 1 - 3")
        exit(1)

    subprocess.call('start python instantiatePRM.py ' + str(nodeId) + ' ' + str(my_ip) + ' ' + str(ip_1) + ' ' + str(ip_2) + '&', shell=True)
    subprocess.call('start python instantiateMap1.py ' + str(nodeId) + ' ' + str(my_ip) + '&', shell=True)
    subprocess.call('start python instantiateMap2.py ' + str(nodeId) + ' ' + str(my_ip) + '&', shell=True)
    subprocess.call('start python instantiateReducer.py ' + str(nodeId) + ' ' + str(my_ip) + '&', shell=True)
    
    #Instantiate CLI
    cli_1 = CLI(str(my_ip), id)


if __name__ == "__main__":
    main()
