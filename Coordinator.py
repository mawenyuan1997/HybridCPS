import sys

import redis
import time
import json
from threading import Thread
import socket

# TODO
import utils


class Coordinator(Thread):

    def __init__(self, addr, port):
        super().__init__()
        self.addr = addr
        self.port = port

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
                        if msg['type'] == 'need switch':
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                                ss.connect(('127.0.0.1', utils.PORT['PA start']))
                                ss.send(json.dumps({'type': 'switch request'}).encode())
                            for port in range(7000, 7037):
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                                    ss.connect(('127.0.0.1', port))
                                    ss.send(json.dumps({'type': 'switch request'}).encode())

        Thread(target=start_listener).start()



if __name__ == "__main__":
    args = sys.argv[1:]
    addr, port = args[0], int(args[1])
    Coordinator(addr, port).start()
