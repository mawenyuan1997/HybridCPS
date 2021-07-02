import sys

import redis
import time
import json
from threading import Thread
import socket

import utils


class Monitor(Thread):

    def __init__(self, addr, port):
        super().__init__()
        self.addr = addr
        self.port = port
        self.cpu_usage = {}
        self.throughput = {}
        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host=utils.IP['pubsub'], port=utils.PORT['pubsub'],
            decode_responses=True, encoding='utf-8'))
        self.sub = self.client.pubsub()
        self.pubsub_queue = []
        self.message_queue = []
        self.sub.subscribe(['performance'])


    def run(self):
        def start_listener():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.addr, self.port))
                s.listen()
                while True:
                    conn, addr = s.accept()
                    with conn:
                        while True:
                            data = conn.recv(1024)
                            if not data:
                                break
                            msg = json.loads(data.decode())
                            if msg['type'] == 'performance data':  # from scheduler
                                node = msg['node']
                                self.cpu_usage[node] = msg['cpu']
                                self.throughput[node] = msg['throughput']
                                print(node, msg['cpu'], msg['throughput'])

        def start_pubsub_listener():
            for m in self.sub.listen():
                if m.get("type") == "message":
                    channel = m['channel']
                    msg = json.loads(m['data'])
                    # self.pubsub_queue.append((channel, msg))
                    if channel == 'performance' and msg['type'] == 'performance data':  # from agents
                        node = msg['node']
                        self.cpu_usage[node] = msg['cpu']
                        self.throughput[node] = msg['throughput']
                        print(node, msg['cpu'], msg['throughput'])

        Thread(target=start_listener).start()
        Thread(target=start_pubsub_listener).start()


if __name__ == "__main__":
    args = sys.argv[1:]
    addr, port = args[0], int(args[1])
    Monitor(addr, port).start()
