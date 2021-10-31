import sys

import redis
import time
import json
from threading import Thread
import socket
import utils
from utils import send_msg
from utils import distance


class CentralController(Thread):

    def __init__(self, n):
        super().__init__()
        self.n = n

    def run(self):
        for i in range(self.n):
            Thread(target=self.send_control, args=(i,)).start()

    def send_control(self, node):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            ip, port = utils.IP['Node' + str(node)], 4000
            s.connect((ip, port))
            for i in range(50):
                s.send(json.dumps({'velocity': (10, 10)}).encode())
                time.sleep(1.0 / 50)

if __name__ == "__main__":
    args = sys.argv[1:]
    n = int(args[0])
    CentralController(n).start()