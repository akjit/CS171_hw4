
import os
import sys
import socket

class Reducer(object):

    def __init__(self, nodeId, IP, Port):
        self.nodeId = nodeId
        self.ip = str(IP)
        self.port = int(Port)
        self.listeningSocket = None

        self.startListener()
        self.processCommands()

    def startListener(self):
        self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listeningSocket.bind( (self.ip, self.port) )
        self.listeningSocket.listen(5)
        print("Node " + str(self.nodeId) + " Reducer:Listening socket (" + str(self.ip) + "," + str(self.port) + ") ready\n")

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
                    if command != '':
                        if command.split()[0] == "reduce":
                            filename_1 = command.split()[1]
                            filename_2 = command.split()[2]
                            print("Reducer processing: " + str(filename_1) + " " + str(filename_2))
                            self.reduce(filename_1, filename_2)

    def reduce(self, in_file_1, in_file_2):
        filename = str(in_file_1).split("_")[0]

        file_read_1 = open(str(in_file_1), 'r')
        file_read_2 = open(str(in_file_2), 'r')
        file_write = open(str(filename) + "_reduced.txt", 'w')
        
        final_list = {}

        for line in file_read_1:
            pairs = line.split(" ")
            for pair in pairs:
                if str(pair) != '':
                    pair = str(pair).replace("(", '')
                    pair = pair.replace(")", '')
                    keyvalue = pair.split(",")
                    key = keyvalue[0]
                    value = keyvalue[1]
                    final_list[key] = int(value)
        
        for line in file_read_2:
            pairs = line.split(" ")
            for pair in pairs:
                if str(pair) != '':
                    pair = str(pair).replace("(", '')
                    pair = pair.replace(")", '')
                    keyvalue = pair.split(",")
                    key = keyvalue[0]
                    value = keyvalue[1]
                    if key not in final_list:
                        final_list[key] = int(value)
                    else:
                        final_list[key] = final_list[key] + int(value)

        for key, value in final_list.items():
            file_write.write("(" + str(key) + "," + str(value) + ") ")
                

        file_read_1.close()
        file_read_2.close()
        file_write.close()
            
