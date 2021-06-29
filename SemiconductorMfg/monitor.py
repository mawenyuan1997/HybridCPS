import sys

import redis
import time
import json
from threading import Thread
import socket

# TODO
import utils


class Monitor(Thread):

    def __init__(self, addr, port):
        super().__init__()
        self.addr = addr
        self.port = port
        self.cpu_usage = {}
        self.throughput = {}

    def run(self):
        def start_listener():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.addr, self.port))
                s.listen()
                conn, addr = s.accept()
                with conn:
                    while True:
                        data = conn.recv(1024)
                        if not data:
                            break
                        msg = json.loads(data.decode())
                        if msg['type'] == 'performance data':
                            node = msg['node']
                            self.cpu_usage[node] = msg['CPU']
                            self.throughput[node] = msg['throughput']



        Thread(target=start_listener).start()



if __name__ == "__main__":
    args = sys.argv[1:]
    addr, port = args[0], int(args[1])
    Monitor(addr, port).start()
