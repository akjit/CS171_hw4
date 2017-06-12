#!/usr/bin/env python3

import sys
import threading
import socket
import time
import math
from log import log

class PRM(object):

        def __init__(self, nodeId, prmIp, prmIp_2, prm_Ip3, prmPort):
                self.nodeId = str(nodeId)
                self.prmIp = str(prmIp)
                self.prmIp_2 = str(prmIp_2)
                self.prmIp_3 = str(prmIp_3)
                self.prmPort = int(prmPort)
                self.pause = False
                self.logs = []
                
                #hardcode logs for test
                log1 = log()
                log2 = log()
                log3 = log()
                log1.setLogFromFilename("words_reduced.txt")
                log2.setLogFromFilename("words2_reduced.txt")
                log3.setLogFromFilename("words3_reduced.txt")
                self.logs.append(log1)
                self.logs.append(log2)
                self.logs.append(log3)

                self.outgoingPRM_itself = None
                self.outgoingPRM_2 = None
                self.outgoingPRM_3 = None

                self.startListener()           
                self.startOutgoing()           
                self.processCommands()

        def startOutgoing(self):
                connected = False
                while connected != True:
                        try:
                                self.outgoingPRM_itself = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                                self.outgoingPRM_itself.connect( (self.prmIp, self.prmPort) )
                                print("Node " + str(self.nodeId) + ": Outgoing socket for its own PRM (" + str(self.prmIp) + "," + str(self.prmPort) + ") ready\n")
                                connected = True
                        except socket.error:
                                print("Can't connect")
                                time.sleep(2)

                connected = False
                while connected != True:
                        try:
                                self.outgoingPRM_2 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                                self.outgoingPRM_2.connect( (self.prmIp_2, self.prmPort) )
                                print("Node " + str(self.nodeId) + ": Outgoing socket for its 2nd PRM (" + str(self.prmIp_2) + "," + str(self.prmPort) + ") ready\n")
                                connected = True
                        except socket.error:
                                print("Can't connect")
                                time.sleep(2)
                                
                connected = False
                while connected != True:
                        try:
                                self.outgoingPRM_3 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                                self.outgoingPRM_3.connect( (self.prmIp_3, self.prmPort) )
                                print("Node " + str(self.nodeId) + ": Outgoing socket for its 3rd PRM (" + str(self.prmIp_3) + "," + str(self.prmPort) + ") ready\n")
                                connected = True
                        except socket.error:
                                print("Can't connect")
                                time.sleep(2)

        
        def startListener(self):
                self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.listeningSocket.bind( (self.prmIp, self.prmPort) )
                self.listeningSocket.listen(5)
                print("Node " + str(self.nodeId) + " PRM: Listening socket (" + str(self.prmIp) + "," + str(self.prmPort) + ") ready\n")

        def cli_total(self):
                total = 0
                for log in self.logs:
                        for key,value in log.dict.items():
                                total = total + int(value)
                print(str(total))
                print("\n")

        def cli_print(self):
                for log in self.logs:
                        print(str(log.filename))
                print("\n")

        def cli_merge(self, pos1, pos2):
                #beginning at 1 ...
                final_list = {}

                for key,value in self.logs[int(pos1) - 1].dict.items():
                        final_list[key] = int(value)

                for key,value in self.logs[int(pos2) - 1].dict.items():
                        if key not in final_list:
                                final_list[key] = int(value)
                        else:
                                final_list[key] = final_list[key] + int(value)

                for key, value in final_list.items():
                        print("(" + str(key) + "," + str(value) + ")")
                print("\n")
                
        def processCommands(self):
                connection, addr = self.listeningSocket.accept()
                while True:
                        try:
                                data = connection.recv(1024)
                        except socket.timeout:
                                break
                        except socket.error:
                                print("Error occurred when check for command")
                                break
                        else:
                                command = str(data.decode())
                                if(command.split()[0] == "replicate"):
                                        if(self.pause == False):
                                                #replicate freely
                                                print("\nPRM replicate not yet finished\n")
                                        elif(self.pause == True):
                                                print("\nSorry, PRM is paused.\n")
                                elif(command.split()[0] == "stop"):
                                        self.pause = True
                                        print("\nPRM paused!\n")
                                elif(command.split()[0] == "resume"):
                                        self.pause = False
                                        print("\nPRM resumed!\n")
                                elif(command.split()[0] == "total"):
                                        self.cli_total()
                                elif(command.split()[0] == "print"):
                                        self.cli_print()
                                elif(command.split()[0] == "merge"):
                                        if len(command.split()) == 3:
                                                self.cli_merge(command.split()[1], command.split()[2])
                                        else:
                                                print("\nIncorrect number of arguments for merge command\n")
                                else:
                                        print("\nSorry, invalid command. ")

        
                                

