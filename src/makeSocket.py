import glob
import sys
import os
import socket
import time
import hashlib
import threading
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

def check_port(ip,port_number):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((ip, port_number))
    if result == 0:
        return True
    else:
        return False
    
def connect(ip_address, port):
    if(check_port(ip_address,port)):
        transport = TSocket.TSocket(ip_address, port)
        transport.setTimeout(5000)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        file_store = keyStore.Client(protocol)
        transport.open()
        return file_store, transport
    else:
        raise RuntimeError("server Down")