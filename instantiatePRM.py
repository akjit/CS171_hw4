import os
import sys
import socket
from log import log
from cli import CLI
from prm import PRM

def main():
    nodeId = sys.argv[1]
    ip1 = sys.argv[2]
    ip2 = sys.argv[3]
    ip3 = sys.argv[4]
    
    #instantiate PRMs
    prm_1 = PRM(nodeId, ip1, ip2, ip3, 5001)


if __name__ == "__main__":
    main()
