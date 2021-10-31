import sys

import redis
import time
import json
from threading import Thread
import socket
import utils
from utils import send_msg
from utils import distance


class Robot(Thread):

    def __init__(self, ip, port):
        super().__init__()
        self.ip = ip
        self.port = port
        self.message_queue = []
        self.receive_time = []

    def run(self):
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
                            self.receive_time.append(time.time())

        Thread(target=start_socket_listener).start()
        time.sleep(14)
        print('total time: {}'.format(max(self.receive_time) - min(self.receive_time)))
        print('total receive: {}'.format(len(self.receive_time)))
        self.receive_time.sort()
        tail_latency = 0
        for i in range(len(self.receive_time) - 1):
            tail_latency = max(tail_latency, self.receive_time[i+1] - (self.receive_time[0] + i/50.0))
        print('teil latency: {}'.format(tail_latency))
        j = 0
        miss = 0
        for i in range(len(self.receive_time) - 1):
            while j < len(self.receive_time) and self.receive_time[j] < self.receive_time[0] + i / 50.0:
                j += 1
            if self.receive_time[j] > self.receive_time[0] + (i + 1) / 50.0:
                miss += 1
        print('miss: {}'.format(miss))


if __name__ == "__main__":
    args = sys.argv[1:]
    ip = args[0]
    port = int(args[1])
    Robot(ip, port).start()