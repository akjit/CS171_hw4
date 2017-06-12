import os
import sys
import socket
from log import log
from cli import CLI
from prm import PRM
from map import Mapper

def main():
    nodeId = sys.argv[1]
    ip = sys.argv[2]
    
    #instantiate Map1
    prm_1 = Mapper(nodeId, ip, 5002, 1)
        
        

if __name__ == "__main__":
    main()
