#!/usr/bin/env python3

"""
Abhijit Kulkarni // Jordan Ang
CS171 6/2017 MapReduce assignment

Command Line Environment

In this assignment, we are essentially given many text files. We will process unique word counts
from these text files using two mappers and a dictionary. Each mapper will process one half of the file.
This function of getting word counts is not only local, but can happen in other machines. Thus, there is a
reducer that takes two different files (perhaps created by different machines), and then combines the counts of
words that appear in both files. Users can submit commands through the CLI, which then sends a request to either
the mappers, the reducer, or the PRM. The PRM is used to maintain consensus in the file order received from reduce.
It will replicate the file in the correct order, across all machines.

This file deals with user I/O for the CLI, and offers these functions:

1)      map f1
2)      reduce f1 f2
3)      replicate f1
4)      stop
5)      resume
6)      total pos1 pos2
7)      print
8)      merge pos1 pos2

For part 1 we only need replicate, stop, resume, total, print, and merge to work

"""

import sys
import time
import socket
import threading
import math
import os
import sys
import socket
from log import log
from prm import PRM
from map import Mapper
from reducer import Reducer

class CLI(object):
        
        def __init__(self, IP):
                self.prmIP = str(IP)
                self.prmPort = int(5001)
                self.prmSocket = None

                self.map1IP = str(IP)
                self.map1Port = int(5002)
                self.map1Socket = None

                self.map2IP = str(IP)
                self.map2Port = int(5003)
                self.map2Socket = None

                self.reduceIP = str(IP)
                self.reducePort = int(5004)
                self.reduceSocket = None

                self.startOutgoing()

                self.commands()

        def startOutgoing(self):
                self.prmSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                self.prmSocket.connect( (self.prmIP, self.prmPort) )
                print("Outgoing socket for PRM (" + str(self.prmIP) + "," + str(self.prmPort) + ") ready\n")
                
                self.map1Socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.map1Socket.connect( (self.map1IP, self.map1Port) )
                print("Outgoing socket for map1 (" + str(self.map1IP) + "," + str(self.map1Port) + ") ready\n")
                
                self.map2Socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.map2Socket.connect( (self.map2IP, self.map2Port) )
                print("Outgoing socket for map2 (" + str(self.map2IP) + "," + str(self.map2Port) + ") ready\n")
                
                self.reduceSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.reduceSocket.connect( (self.reduceIP, self.reducePort) )
                print("Outgoing socket for reduce (" + str(self.reduceIP) + "," + str(self.reducePort) + ") ready\n")

        def commands(self):
                command = ""
                print("\n\n\nMap Reduce Application for CS171, developed by Abhijit Kulkarni and Jordan Ang. \n")
                while(1):
                        command = input("Please enter your command according to the format: \n")
                        if(command.split()[0] == "map"):
                                if len(command.split()) == 4:
                                        file_name = command.split()[1]
                                        offset = command.split()[2]
                                        size = command.split()[3]
                                        half_size = (int(size) - int(offset)) / 2
                                        range_1 = int(offset) + int(half_size)
                                        range_2 = int(size)
                                        params1 = str(file_name) + " " + str(offset) + " " + str(range_1)
                                        params2 = str(file_name) + " " + str(range_1) + " " + str(range_2)
                                        self.map1Socket.send(str(params1).encode())
                                        self.map2Socket.send(str(params2).encode())
                                else:
                                        print("\nIncorrect number of arguments for map command\nmap {filename} {offset} {input_size}\n")
                        elif(command.split()[0] == "reduce"):
                                if len(command.split()) == 3:
                                        self.reduceSocket.send(str(command).encode())
                                else:
                                        print("\nSorry, reduce {file1} {file2} is not supported yet. \n")
                        elif(command.split()[0] == "exit"):
                                print("\nGoodbye!\n")
                                exit(1)
                        elif(command.split()[0] == "replicate" or command.split()[0] == "stop" or command.split()[0] == "resume" or command.split()[0] == "total" or command.split()[0] == "print" or command.split()[0] == "merge"):
                                self.prmSocket.send(str(command).encode())
                        else:
                                print("Sorry, invalid command.\n")

        




