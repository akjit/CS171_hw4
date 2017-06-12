import os
import sys
import socket
from log import log
from cli import CLI
from prm import PRM
from map import Mapper
from reducer import Reducer

def main():
    nodeId = sys.argv[1]
    ip = sys.argv[2]
    
    #instantiate reduce
    reduce = Reducer(nodeId, ip, 5004)
        
        

if __name__ == "__main__":
    main()
