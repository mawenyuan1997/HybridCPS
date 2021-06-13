import sys

import redis
import time
import json
from threading import Thread
import socket
import os

import utils


class JobCreator(Thread):

    def __init__(self, addr, port, coord_addr, coord_port):
        super().__init__()
        self.addr = addr
        self.port = port
        self.cord_addr = coord_addr
        self.cord_port = coord_port

    def run(self):
        pa_dir = 'HybridCPS/SemiconductorMfg/PAconfig/'
        for pa_file in os.listdir(pa_dir):
            pa_name = pa_file.split('.')[0]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                ip, port = self.cord_addr, self.cord_port
                s.connect((ip, port))
                s.send(json.dumps({'type': 'new product agent',
                                   'PA name': pa_name,
                                   'PA file directory': pa_dir + pa_name}).encode())


if __name__ == "__main__":
    args = sys.argv[1:]
    addr, port = args[0], int(args[1])
    cord_addr, cord_addr = args[2], int(args[3])
    JobCreator(addr, port, cord_addr, cord_addr).start()
