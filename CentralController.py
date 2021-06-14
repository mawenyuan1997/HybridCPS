import sys

import redis
import time
import json
from threading import Thread
import socket

import utils


class CentralController(Thread):

    def __init__(self, ip, port):
        super().__init__()
        self.ip = ip
        self.port = port
        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host=utils.IP['pubsub'], port=utils.PORT['pubsub'],
            decode_responses=True, encoding='utf-8'))
        self.sub = self.client.pubsub()
        self.interests = ['location', 'capability']
        self.sub.subscribe(self.interests)

        self.pubsub_queue = []
        self.message_queue = []
        self.listen()

    def optimize(self):
        current_env = {}
        print('current env: {}'.format(current_env))
        # TODO shortest path

    def send_msg(self, addr, msg):
        print(addr)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            ip, port = addr
            s.connect((ip, port))
            s.send(json.dumps(msg).encode())

    def listen(self):
        def start_pubsub_listener():
            for m in self.sub.listen():
                if m.get("type") == "message":
                    # latency = time.time() - float(m['data'])
                    # print('Recieved: {0}'.format(latency))
                    channel = m['channel']
                    msg = json.loads(m['data'])
                    self.pubsub_queue.append((channel, msg))

        def start_socket_listener():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.ip, self.port))
                s.listen()
                while True:
                    conn, addr = s.accept()
                    with conn:
                        while True:
                            data = conn.recv(1024)
                            if not data:
                                break
                            msg = json.loads(data.decode())
                            self.message_queue.append(msg)
                            if msg['type'] == 'plan request':
                                print('CC receive plan request')
                                pa_addr = msg['PA address']
                                self.send_msg(pa_addr, {'type': 'plan',
                                                        'path': [[[40, 68], ['127.0.0.1', 7034], 'RobotM12'],
                                                                 [[40, 72], ['127.0.0.1', 7000], 'BufferB1'],
                                                                 [[30, 100], ['127.0.0.1', 7028], 'RobotB1']
                                                                 ],
                                                        'processing machine': [['127.0.0.1', 7008], 'MachineA']
                                                        })

        Thread(target=start_pubsub_listener).start()
        Thread(target=start_socket_listener).start()


if __name__ == "__main__":
    args = sys.argv[1:]
    ip, port = args[0], int(args[1])
    CentralController(ip, port).start()
