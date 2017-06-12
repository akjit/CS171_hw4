#!/usr/bin/env python3

import sys
import os
import socket
from log import log

class Mapper(object):

    def __init__(self, IP, Port, ID):
        self.ip = str(IP)
        self.port = int(Port)
        self.id = str(ID)
        self.listeningSocket = None
        
        self.startListener()
        self.processCommands()

    def startListener(self):
        self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listeningSocket.bind( (self.ip, self.port) )
        self.listeningSocket.listen(5)
        print("Listening socket (" + str(self.ip) + "," + str(self.port) + ") ready\n")

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
                    filename = command.split()[0]
                    offset = command.split()[1]
                    maxRange = command.split()[2]
                    print("Map #" + str(self.id) + " processing: " + str(filename) + " " + str(offset) + " " + str(maxRange))
                    self.map(filename, offset, maxRange)

    def map(self, in_file, in_offset, in_size):
        file_name = str(in_file)
        offset = int(in_offset)
        size = int(in_size)
        
        file_read = open(str(file_name), 'r')
        file_write = open(str(file_name).split('.')[0] + "_I_" + str(self.id) + ".txt", 'w')
        count_list = {}
        
        for line in file_read:
            word_list = line.split(" ")

            if int(size) > len(word_list):
               print("ERROR: Size is greater than the number of words in the file")
               return

            i = int(offset)
            while i < size:
                word = word_list[i]
                if word_list[i] not in count_list:
                    count_list[str(word).lower()] = 1
                else:
                    count_list[str(word).lower()] = count_list[str(word).lower()] + 1
                i = i + 1

            
        for key, value in count_list.items():
            file_write.write("(" + str(key) + "," + str(value) + ") ")
        
        file_read.close()
        file_write.close()
