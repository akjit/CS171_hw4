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

    subprocess.call('start python instantiatePRM.py', shell=True)
    subprocess.call('start python instantiateMap1.py', shell=True)
    subprocess.call('start python instantiateMap2.py', shell=True)
    subprocess.call('start python instantiateReducer.py', shell=True)
    
    #Instantiate CLIs
    cli_1 = CLI("127.0.0.1")
        
        



if __name__ == "__main__":
    main()
