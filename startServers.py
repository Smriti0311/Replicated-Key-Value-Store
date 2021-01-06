import os
import sys
import threading

def startServer(ipaddress,portNumber):
    cmd = "python src/Server.py " + portNumber
    os.system(cmd)
if __name__ == '__main__':
    run_event = threading.Event()
    run_event.set()
    threads = []
    try:
        with open('nodes.txt') as openfileobject:
            for line in openfileobject:
                address = line.split(":")
                print(address[1][:4])
                threads.append(threading.Thread(target = startServer , args = (address[0],address[1][:4])))
                threads[len(threads)-1].start()
    except KeyboardInterrupt:
        print("attempting to close threads")
        run_event.clear()
        for t in threads:
            t.join()
        print("threads successfully closed")

