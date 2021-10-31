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
        self.receive_time = []

    def run(self):
        def start_socket_listener():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.ip, self.port))
                print(self.ip + ' listen')
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
        time.sleep(3)
        print(50.0 / (max(self.receive_time) - min(self.receive_time)))

if __name__ == "__main__":
    args = sys.argv[1:]
    ip = args[0]
    port = int(args[1])
    Robot(ip, port).start()