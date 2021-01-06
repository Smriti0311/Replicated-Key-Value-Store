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

import sys
import glob
import hashlib
import makeSocket
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])


from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from chord import keyStore
from chord.ttypes import server, SystemException


def main():
    # Make socket
    client, transport = makeSocket.connect(sys.argv[1], int(sys.argv[2]))

    if client.ping():
        # testing write

        print('Setting the co-ordinator')
        client.set_cordinator()
        # client.set_consistency("ONE")
        print('Writing values for different keys')
        print('Default consistency : quorum')
        print(client.write_key(65,"sdfgjhk"))
        print(client.write_key(75,"djfgdk"))
        print('Changing consistency to 1')
        client.set_consistency("ONE")
        print('Writing values with consistency 1 for different keys')
        print(client.write_key(88,"seeurhtuehg"))
        print(client.write_key(98,"sjgnselhg"))
        print(client.write_key(100,"WE GOT IT!!!"))
        print('Changing consistency to quorum')
        client.set_consistency("quorum")
        print('Writing values with consistency quorum for different keys')
        print(client.write_key(54,"tkjyirtjyo"))
        print(client.write_key(255,"sdkjfngksjng"))
        print('Reading a key that exists')
        print(client.read_key(100))
        
        print('Overwriting the value of key that exists')
        print(client.write_key(100, "OVERWRITTEN"))
        print('Reading the overwritten value of a key')
        print(client.read_key(100))

        print('Testing read for a key that does not exist')
        print(client.read_key(25))
    
    # Close!
    client.reset_cordinator()
    transport.close()

if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
