import os
import sys
import socket
from log import log
from cli import CLI
from prm import PRM
from map import Mapper
from reducer import Reducer

def main():
    #instantiate reduce
    reduce = Reducer("127.0.0.1", 5004)
        
        

if __name__ == "__main__":
    main()
