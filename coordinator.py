import os
import sys

import redis
import time
import json
from threading import Thread
import socket

# TODO
import utils
from utils import send_msg


class Coordinator(Thread):

    def __init__(self, addr, port):
        super().__init__()
        self.addr = addr
        self.port = port
        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host=utils.IP['pubsub'], port=utils.PORT['pubsub'],
            decode_responses=True, encoding='utf-8'))
        self.sub = self.client.pubsub()
        self.pubsub_queue = []
        self.message_queue = []
        self.sub.subscribe(['dispatching'])

        self.agent_status = {}
        self.ra_num = 0
        self.pa_num = 0

    def run(self):
        def start_pubsub_listener():
            for m in self.sub.listen():
                if m.get("type") == "message":
                    channel = m['channel']
                    msg = json.loads(m['data'])
                    # self.pubsub_queue.append((channel, msg))
                    if channel == 'dispatching' and msg['type'] == 'need switch':  # from agents
                        # TODO decide switch
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                            ss.connect(('127.0.0.1', utils.PORT['PA start']))
                            ss.send(json.dumps({'type': 'switch request'}).encode())
                        for port in range(7000, 7037):
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                                ss.connect(('127.0.0.1', port))
                                ss.send(json.dumps({'type': 'switch request'}).encode())
                    elif channel == 'dispatching' and msg['type'] == 'plan request':
                        send_msg((utils.IP['scheduler'], utils.PORT['scheduler']), msg)

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
                            if msg['type'] == 'switch advice':  # from monitor
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                                    ss.connect(('127.0.0.1', utils.PORT['PA start']))
                                    ss.send(json.dumps({'type': 'switch request'}).encode())
                                for port in range(7000, 7037):
                                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                                        ss.connect(('127.0.0.1', port))
                                        ss.send(json.dumps({'type': 'switch request'}).encode())
                            elif msg['type'] == 'plan':
                                self.client.publish('dispatching', json.dumps(msg))
                            elif msg['type'] == 'new product':
                                self.agent_status[msg['PA name']] = ('centralized', 'centralized')
                                os.system('python3 ProductAgent.py {} {} {} {} {} {} &'.format(msg['PA name'],
                                                                                               '127.0.0.1',
                                                                                               utils.PORT[
                                                                                                   'PA start'] + self.pa_num,
                                                                                               msg['config file'],
                                                                                               'centralized',
                                                                                               'distributed'))
                                self.pa_num += 1
                            elif msg['type'] == 'new resource':
                                self.agent_status[msg['RA name']] = 'centralized'
                                os.system('python3 ResourceAgent.py {} {} {} {} {} &'.format(msg['RA name'],
                                                                                             '127.0.0.1',
                                                                                             utils.PORT[
                                                                                                 'RA start'] + self.ra_num,
                                                                                             msg['config file'],
                                                                                             'centralized'))
                                self.ra_num += 1

        Thread(target=start_listener).start()
        Thread(target=start_pubsub_listener).start()


if __name__ == "__main__":
    args = sys.argv[1:]
    addr, port = args[0], int(args[1])
    Coordinator(addr, port).start()
