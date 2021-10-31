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
        def send_control(node):
            for i in range(50):
                send_msg((utils.IP['Node' + str(node)], 8000), {'velocity': (10, 10)})
                time.sleep(1.0/50)

        for i in range(self.n):
            Thread(target=send_control, args=(i,)).start()

if __name__ == "__main__":
    args = sys.argv[1:]
    n = int(args[0])
    CentralController(n).start()