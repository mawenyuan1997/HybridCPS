import sys

import redis
import time
import json
from threading import Thread
import socket


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
                    print('Connected by', addr)
                    while True:
                        data = conn.recv(1024)
                        if not data:
                            break
                        msg = json.loads(data.decode())
                        if msg['type'] == 'switch to centralized request':
                            conn.send(json.dumps({'type': 'agree to switch'}).encode())
                            for ra_addr, ra_port in msg['RAs']:
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                                    ss.connect((ra_addr, ra_port))
                                    ss.send(json.dumps({'type': 'switch to centralized request'}).encode())

        Thread(target=start_listener).start()


if __name__ == "__main__":
    args = sys.argv[1:]
    addr, port = args[0], int(args[1])
    Coordinator(addr, port).start()
