#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import glob
import sys
import os
import socket
import time
import hashlib
import threading
import makeSocket
import time
import socket
import os
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from chord import keyStore

from chord.ttypes import server, SystemException, message
import logging
logging.basicConfig(level=logging.DEBUG)

def system_exception(msg):
    exception = SystemException()
    exception.message = msg
    return exception

class FileHandler:
    
    def __init__(self,port):
        self.ready_state = False
        self.log_file_name = "log/"+str(socket.gethostbyname(socket.gethostname()))+"_"+str(port)+".log"
        self.file = None
        self.curr_node = None
        self.server_table = list()
        self.data = dict()
        self.cordinator = False
        self.replicas = 2
        self.quorum = True
        self.one = False
        self.notify_time = 10800
        self.notify_list = []
        self.log = None
        self.recover()


    def check_log_file(self):
        """
        This function checks for a log folder and creates one if it does not exist

        """
        if(os.path.isdir("log") == False):
            try:
                os.mkdir("log")
            except:
                pass
        if (os.path.isfile(self.log_file_name) == False):
            self.file = open(self.log_file_name, "a")

    def check_init(self):
        """"
        This function checks to see if the nodes are initialised

        """
        if(self.curr_node == None):
            raise Exception("\nnodes are not initalized.. \nHINT: Run Init.py nodes.txt")
        if(len(self.server_table) <= 0):
            raise Exception("\nnodes are not initalized.. \nHINT: Run Init.py nodes.txt")

    def ping(self):
        if(self.ready_state == True):
            return True
        else:
            return False

    def recover(self):
        self.check_log_file()
        self.server_table = [None for i in range(256)]
        with open(self.log_file_name) as fp:
            line = fp.readline()
            while line:
                arguments = line.split()
                if(arguments[1] == "write"):
                    if(arguments[2] == "server_table"):
                        s = server()
                        s.ip = arguments[4]
                        s.port = int(arguments[5])
                        self.server_table[int(arguments[3])] = s
                    if(arguments[2] == "curr_node"):
                        s = server()
                        s.ip = arguments[4]
                        s.port = int(arguments[5])
                        self.curr_node = s
                    if(arguments[2] == "data"):
                        self.data[int(arguments[3])] = arguments[4]
                line = fp.readline()
        self.ready_state = True
    
    def set_cordinator(self):
        self.check_init()
        self.cordinator = True

    def reset_cordinator(self):
        self.check_init()
        self.cordinator = False
    
    def set_consistency(self, consistency_level):
        """ 
        This function determines the consistency level of the key-value pair in the distributed system
        DEFAULT consistency level: Quorum

        Args:
            consistency_level ([string]): "quorum": for global consistency that would update in replicas as well
                                          "one"   : for single consistency that would just update in partioner
        """
        self.check_init()
        if(consistency_level.lower() == "quorum"):
            self.quorum = True
            self.one = False
        if(consistency_level.lower() == "one"):
            self.quorum = False
            self.one = True
        
    def put_log(self, time, operation, data, key, value):
        """
        This function adds to the log file

        """
        self.check_log_file()
        self.file = open(self.log_file_name, "a")
        self.file.write(time+" "+operation+" "+data+" "+key+" "+value+ "\n")
        self.file.close()

    def find_replicas(self, server):
        """
        This  function finds the replicas given a particular server and returns those replicas as a list
        
        """
        self.check_init()
        replica_list = []
        index = self.server_table.index(server)
        for server_index in range(index+1,len(self.server_table)):
            if(self.server_table[server_index].ip != '-1'):
                if(len(replica_list) <= self.replicas):
                    replica_list.append(self.server_table[server_index])
            if(len(replica_list) == self.replicas):
                break
        return replica_list
            
    def find_server(self,key):
        """
        This function finds which server a key belongs to, given a key

        """
        self.check_init()
        for server_index in range(key,len(self.server_table)):
            if(self.server_table[server_index].ip != '-1'):
                return self.server_table[server_index]
        for server_index in range(0,key):
            if(self.server_table[server_index].ip != '-1'):
                return self.server_table[server_index]
    
    def find_next_server(self,server):
        """
        This server finds the next server given a server
        
        """
        index = self.server_table.index(server)
        for server_index in range(index,len(self.server_table)):
            if(self.server_table[server_index].ip != '-1'):
                return self.server_table[server_index]
        for server_index in range(0,index):
            if(self.server_table[server_index].ip != '-1'):
                return self.server_table[server_index]
        
    
    def try_again_replicas(self):
        """
        This function starts any of the replicas which were down and is back 
        """
        self.check_init()
        if(len(self.notify_list) == 0):
            return
        for notify in self.notify_list:
            threading.Thread(target = self.check_if_avilable, args = (notify, )).start()
            self.notify_list.remove(notify)
        
    def check_if_avilable(self, notify):
        """
        This function checks if the replicas are available
        
        """
        self.check_init()
        try:
            client, transport = makeSocket.connect(notify['ip'], notify['port'])
            result = client.ping()
            while result != True:
                time.sleep(1)
                notify['time'] +=1
                result = client.ping()
                if(notify['time'] == self.notify_time):
                    break
            if(client.ping()):
                client.forceful_write_key(notify['key'],notify['value'])
            transport.close()
        except RuntimeError:
            time.sleep(1)
            notify['time'] +=1
            if(notify['time'] == self.notify_time):
                return
            self.check_if_avilable(notify)

    def notify_replicas(self, replica_list, key, value):
        """
        This function notifies the replicas
        """
        self.check_init()
        for replicas in replica_list:
            client,transport = makeSocket.connect(replicas.ip,replicas.port)
            if(client .ping()):
                client.forceful_write_key(key,value)
            else:
                self.notify_list.append({'ip':replicas.ip, 'port': replicas.port, 'key': key, 'value': value, 'time': 0})
            transport.close()
            self.try_again_replicas()
    
    def setServerTable(self, server_table, curr_node):
        """
        This function adds to the log
        """
        self.curr_node = server_table[curr_node]
        self.server_table = server_table 
        self.check_log_file()
        self.put_log(str(time.time()), "write", "curr_node", "-1", str(self.curr_node.ip)+" "+str(self.curr_node.port))
        for server_index in range(0,len(self.server_table)):
            self.put_log(str(time.time()), "write", "server_table", str(server_index), str(self.server_table[server_index].ip)+" "+str(self.server_table[server_index].port))

    def write_key(self,key,value):
        """
        This  function  performs writes, i.e, the put(key, value) operation
        
        """
        if key < 0 or key > 255:
            return "invalid key"
        self.check_init()
        server = self.find_server(key)
        if(server.ip == self.curr_node.ip and server.port == self.curr_node.port):
            self.data[key] = value
            self.put_log(str(time.time()), "write", "data", str(key), str(value))
        else:
            try:
                client, transport = makeSocket.connect(server.ip,server.port)
                if(client.ping()):
                    client.write_key(key,value)
                else:
                    self.notify_list.append({'ip':server.ip, 'port': server.port, 'key': key, 'value': value, 'time': 0})
                    self.try_again_replicas()
                transport.close()
            except RuntimeError:
                self.notify_list.append({'ip':server.ip, 'port': server.port, 'key': key, 'value': value, 'time': 0})
                self.try_again_replicas()
        if(self.cordinator and self.quorum):
            self.notify_replicas(self.find_replicas(server), key, value)
        return "True"

    def forceful_write_key(self,key,value):
        self.check_init()
        self.data[key] = value
        self.put_log(str(time.time()), "write", "data", str(key), str(value))

    def return_value(self,key):
        """
        This function checks if a key exists in order to be read
        """
        if(key in self.data.keys()):
            self.put_log(str(time.time()), "read", "data", str(key), "")
            message = str(self.data[key])
        else:
            message = "Cannot find key, key does not exist"
        return message

    def read_key(self,key):
        """
        This function performs reads, i.e., the get(key) operation
        
        """
        self.check_init()
        server = self.find_server(key)
        if(server.ip == self.curr_node.ip and server.port == self.curr_node.port):
            return self.return_value(key)  
        else:
            try:
                client, transport = makeSocket.connect(server.ip,server.port)
            except RuntimeError:
                server = self.find_next_server(server)
                try:
                    client, transport = makeSocket.connect(server.ip,server.port)
                except RuntimeError:
                    server = self.find_next_server(server)
                    try:
                        client, transport = makeSocket.connect(server.ip,server.port)
                    except RuntimeError:
                        return "All the server are down"
            message = client.return_value(key)
            transport.close()
            return message


if __name__ == '__main__':
    try:
        handler = FileHandler(port = sys.argv[1])
        processor = keyStore.Processor(handler)
        transport = TSocket.TServerSocket(port=int(sys.argv[1]))
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
        server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
        print('Starting the server...')
        server.serve()
        print('done.')
    except KeyboardInterrupt:
        print('Server stopped...')
