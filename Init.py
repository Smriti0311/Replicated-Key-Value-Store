import sys
import socket
import glob
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from chord import keyStore
from chord.ttypes import server
class Init:
    def __init__(self,m):
        self.m = m
        self.no_of_keys = 2**m
        self.server_table = list()
        self.server_list = []
        for i in range(self.no_of_keys):
            x = server()
            x.ip = '-1'
            x.port = -1
            self.server_table.append(x)

    def add_server(self,ip,port):
        self.server_list.append({'ip':ip,'port': port})

    def get_server_table(self):
        n = int(self.no_of_keys / len(self.server_list))
        server = 0
        for i in range(n,self.no_of_keys,n):
            try:
                self.server_table[i].ip = self.server_list[server]['ip']
                self.server_table[i].port = int(self.server_list[server]['port'])
                server+=1
            except:
                print("no more servers")
                break
        return self.server_table
    
    def notifyAll(self):
        self.server_table = self.get_server_table()
        for server in range(len(self.server_table)):
            if(self.server_table[server].ip != '-1'):
                ip = self.server_table[server].ip
                port = self.server_table[server].port
                transport = TSocket.TSocket(ip , int(port))
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = keyStore.Client(protocol)
                transport.open()
                print("notifying ",self.server_table[server])
                client.setServerTable(self.server_table, server)
                client.ping()
                transport.close()



if __name__ == '__main__':
    init = Init(8)
    file_name = sys.argv[1]
    try:
        with open(file_name) as openfileobject:
            for line in openfileobject:
                address = line.split(":")
                address[1] = address[1].replace("\n","")
                init.add_server(address[0],address[1])
    except Exception as msg:
        print("error in the file or file doesnot exist ",msg)
    init.notifyAll()
                