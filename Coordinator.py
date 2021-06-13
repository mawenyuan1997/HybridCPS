import sys

import redis
import time
import json
from threading import Thread
import socket

# TODO
import utils


class Coordinator(Thread):

    def __init__(self, addr, port, nodes):
        super().__init__()
        self.addr = addr
        self.port = port
        self.nodes = nodes

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
                        if msg['type'] == 'switch to centralized request':
                            conn.send(json.dumps({'type': 'agree to switch'}).encode())
                            for ra_addr, ra_port in msg['RAs']:
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                                    ss.connect((ra_addr, ra_port))
                                    ss.send(json.dumps({'type': 'switch to centralized request'}).encode())
                        elif msg['type'] == 'new product agent':
                            pa_name = msg['PA name']
                            pa_dir = msg['PA file directory']
                            self.nodes[0].cmd('python3 HybridCPS/ProductAgent.py {} {} {} {} &'.format(pa_name,
                                                                                                       utils.IP['Node1'],
                                                                                                       utils.PORT['PA start'],
                                                                                                       pa_dir))

        Thread(target=start_listener).start()


if __name__ == "__main__":
    args = sys.argv[1:]
    addr, port = args[0], int(args[1])
    nodes = args[2]
    Coordinator(addr, port, nodes).start()
