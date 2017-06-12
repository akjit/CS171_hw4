#!/usr/bin/env python3

import sys
import threading
import socket
import time
import math
from log import log


q = {}

class PaxosInstance(object):
        '''This is a paxos node that does all 3 roles.'''
        '''Each one of these nodes corresponds to one index stored in the PRM, so each index corresponds to one Log object'''

        global q
        def __init__(self, PRM_id, PRM_IP, PRM_port, outgoing_channels,index, proposed_log=None): 

                self.state = "INITIAL"                          #Current state of currently proposing, currently accepting, currently decided
                                                                                        # {INITIAL, PROPOSED, ACCEPTED, DECIDED}
                #self.file = PRM_file                           #The file that contains all ports for outgoing conns, not sure if necessary
                self.ballot_num = [0,0]                         #Tuple: current suggestion count, current suggested id
                self.accept_num = [0,0]                         #Tuple: current accepted count, current accepted id
                self.index = index                                              #Index of the current paxos instance in the PRM array ???
                self.accepted_log = None                                        #Will be Log object of the current accepted log by this node
                self.proposed_log = proposed_log                        #The log object of the potential log that replicate wants to happen
                self.num_proposals_granted = 0                          #Majority accept?
                self.num_decides_granted = 0
                self.num_peers = 3                                      #Total size
                self.process_id = PRM_id
                self.outgoing_channels = outgoing_channels
                self.peers_who_have_accepted = {}       #key is peer id, value is the log from that peer
                self.prm_conns = {}                                                     #key is PRM id, value is socket

                #self.peers_who_have_decided = {}                       #key is ballot, value is the log



        def sendall(self, message):
                for IP, sock in self.outgoing_channels.items():
                        print(str(message))
                        sock.send(str(message).encode())

        def send_ballot(self, prop_log):
                '''Send a suggestion to all peers'''

                self.state = "PROPOSED"
                print("\nI have received the send ballot.")

                self.ballot_num[0] += 1
                self.ballot_num[1] = self.process_id
                self.proposed_log = prop_log
                self.index = index

                message = Message(Message.PREPARE_T, self.index, self.ballot_num)
                self.sendall(message)


        def response_ballot(self, message):
                '''Receive ballot and decide to send nack or send accept according to ID'''

                prop_ballot = message.ballot_num
                if self.index == None:
                        self.index = message.index

                if self.state == "DECIDED":
                        return                                  #we have already decided on a value so we won't be listening to any responses.

                if (prop_ballot[0] > self.ballot_num[0]) or (prop_ballot[0] == self.ballot_num[0] and prop_ballot[1] > self.ballot_num[1]):
                        # if count is greater for the proposed ballot, or if the count is equal but the PID is greater for the prop ballot

                        self.ballot_num = prop_ballot
                        if self.index == None:
                                self.index = message.index
                        
                        outgoing_msg = Message(Message.ACK_T, self.index, prop_ballot, self.accept_num, self.accepted_log)
                        #outgoing_msg = "ack " + str(prop_ballot[0]) + " " + str(prop_ballot[1]) + " " + str(accept_num[0]) + " " + str(accept_num[1]) + " " + str(accepted_log) + "\n"


                        # send outgoing_msg to the original sender by using information in the message parameter
                        self.outgoing_channels[self.outgoing_channels[prop_ballot[1]]].send(str(outgoing_msg).encode())

                return
                
                #else just ignore the message since the ballot value is lower

        def recv_proposal_granted(self, message):
                '''Receive a good-to-go proposal message from peer, check to send accepts out'''

                self.peers_who_have_accepted[message.accept_num] = message.log
                self.num_proposals_granted += 1

                if(self.num_proposals_granted > self.num_peers/2):
                        empty = True
                        max_info = [[0,0],log()]
                        for acc_num, log in self.peers_who_have_accepted.items():
                                if(log.filename != "*"):
                                        empty = False
                                        if (acc_num[0] > max_info[0][0]) or (acc_num[0] == max_info[0][0] and acc_num[1] > max_info[0][1]):
                                                max_info = [acc_num,log]

                        if empty:
                                self.accepted_log = self.proposed_log
                        else:
                                self.accepted_log = max_info[1]

                        #accept_msg = "accept " + str(self.ballot_num[0]) + " " + str(self.ballot_num[1]) + " " + str(self.accepted_log) + " ||"
                        accept_msg = Message(Message.ACCEPT_T, self.index, self.ballot_num, self.accepted_log)
                        self.sendall(accept_msg)

                return

        def response_accept_proposal(self, message):

                # self.state = "ACCEPTED"
                # decide_msg = Message(Message.DECIDE_T, message.ballot_num, message.index, message.log)
                # self.sendall(decide_msg)
                if self.state == "DECIDED":
                        return

                self.state == "ACCEPTED"

                prop_ballot = message.ballot_num

                if(prop_ballot == self.ballot_num):
                        # if we have reached the original sender who just got an accept message from a peer
                        self.num_accepts_granted += 1
                        if(self.num_accepts_granted > self.num_peers/2):
                                self.accepted_log = message.log
                                self.index = message.index
                                decision_msg = Message(Message.DECIDE_T, self.ballot_num, self.index, self.accepted_log)
                                self.sendall(decision_msg)

                elif ((prop_ballot[0] > self.ballot_num[0]) or (prop_ballot[0] == self.ballot_num[0] and prop_ballot[1] > self.ballot_num[1])):
                        # if count is greater for the proposed ballot, or if the count is equal but the PID is greater for the prop ballot
                        self.accept_num = prop_ballot
                        self.accepted_log = message.log 
                        self.num_proposals_granted = 0
                        self.num_accepts_granted = 0
                        self.index = message.index
                        self.sendall(message)

                return

        def recv_decision(self, message):
                if message.log == self.accepted_log:
                        self.num_decides_granted += 1

                if(self.num_decides_granted > self.num_peers/2):
                        self.accepted_log = message.log
                        self.state = "DECIDED"
                        self.index = message.index
                        return self.accepted_log, self.index
                return


class PRM(object):

        global q 

        def __init__(self, nodeId, prmIp, prmIp_2, prmIp_3, prmPort):
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

                self.paxosInstance_itself = None
                self.paxosInstance_2 = None
                self.paxosInstance_3 = None

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
                                print("Node " + str(self.nodeId) + " can't connect to ip: " + str(self.prmIp_2))
                                time.sleep(2)
                                
                connected = False
                while connected != True:
                        try:
                                self.outgoingPRM_3 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                                self.outgoingPRM_3.connect( (self.prmIp_3, self.prmPort) )
                                print("Node " + str(self.nodeId) + ": Outgoing socket for its 3rd PRM (" + str(self.prmIp_3) + "," + str(self.prmPort) + ") ready\n")
                                connected = True
                        except socket.error:
                                print("Node " + str(self.nodeId) + " can't connect to ip: " + str(self.prmIp_3))
                                time.sleep(2)

        
        def startListener(self):
                self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.listeningSocket.bind( (self.prmIp, self.prmPort) )
                self.listeningSocket.listen(5)
                print("Node " + str(self.nodeId) + " PRM: Listening socket (" + str(self.prmIp) + "," + str(self.prmPort) + ") ready\n")

        def cli_total(self):
                print("reached total!\n")
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

        def cli_replicate(self, data):
                proposed_log = log()
                filename = stringdata.split(" ")[0]                                                                                                                     #update when we have map reduce to be the actual Log object generated by reduce
                proposed_log.setLogFromFilename(filename)
                self.log_index += 1
                self.currPaxosInstance = PaxosInstance(PRM_id, PRM_file, PRM_IP, PRM_port, self.site_connections,self.log_index, proposed_log)
                self.currPaxosInstance.send_ballot(proposed_log)
                
        def processCommands(self):
                connection, addr = self.listeningSocket.accept()
                while True:
                        print("reached here")
                        try:
                                data = connection.recv(1024)
                        except socket.timeout:
                                break
                        except socket.error:
                                print("Error occurred when check for command")
                                break
                        else: 
                                command = str(data.decode())
                                print(str(command))
                                if command != '':
                                        if(command.split()[0] == "replicate"):
                                                if(self.pause == False):
                                                        #replicate freely
                                                        self.cli_replicate(command.split()[1])
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

        
                                

