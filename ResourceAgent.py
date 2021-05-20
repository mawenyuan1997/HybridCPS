import redis
import time
import json
import threading
import numpy as np
from threading import Thread
import sys
import socket


class ResourceAgent(Thread):

    def __init__(self, name, pos, tasks):
        super().__init__()
        self.name = name
        self.pos = pos
        self.tasks = tasks
        self.data = {'position': self.pos, 'capability': self.tasks}

        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host='192.168.1.100', port=6379,
            decode_responses=True, encoding='utf-8'))

        self.sub = self.client.pubsub()
        self.sub.subscribe(self.tasks.keys())

        self.need_transition = True

        self.message_queue = []
        self.listen()

    def send_command_and_wait(self, task, origin):
        if task == 'A':
            task_duration = 20 if self.name == 'RA1' else 10
        else:
            task_duration = 10
        task_duration += abs(origin[0] - self.pos[0]) + abs(origin[1] - self.pos[1])
        time.sleep(np.random.normal(task_duration, 2))

    def wait_for_task(self):
        print('{} wait for task'.format(self.name))
        while True:
            while self.message_queue:
                channel, msg = self.message_queue.pop(0)
                if msg['type'] == 'announcement':
                    return channel, msg['PA name'], msg['current position']

    def send_bid(self, task, origin):
        print('{} send bid to task {}'.format(self.name, task))
        if task == 'A':
            task_duration = 20 if self.name == '1' else 10
        else:
            task_duration = 10
        task_duration += abs(origin[0] - self.pos[0]) + abs(origin[1] - self.pos[1])
        self.client.publish(task, json.dumps({'time': time.time(),
                                              'type': 'bid',
                                              'RA name': self.name,
                                              'position': self.pos,
                                              'finish time': task_duration
                                              }))

    def wait_for_confirm(self, task, PA):
        print('{} wait for task {} confirm from {}'.format(self.name, task, PA))
        start = time.time()
        while time.time() - start < 10:
            while self.message_queue:
                channel, msg = self.message_queue.pop(0)
                if msg['type'] == 'bid confirm' and channel == task and msg['PA name'] == PA:
                    return msg['RA name'] == self.name
        print('{} timeout wait for confirm'.format(self.name))
        return False

    def send_finish_ack(self, task, PA):
        print('{} send task {} finish ack'.format(self.name, task))
        now = time.time()
        self.client.publish(task, json.dumps({'time': now,
                                              'type': 'finish ack',
                                              'PA name': PA
                                              }))

    def switch_to_centralized(self):
        self.sub.unsubscribe(self.tasks.keys())
        self.centralized_mode()

    def distributed_mode(self):
        while True:
            task, PA, current_pos = self.wait_for_task()
            self.send_bid(task, current_pos)
            bid_accept = self.wait_for_confirm(task, PA)
            if bid_accept:
                self.send_command_and_wait(task, current_pos)
                self.send_finish_ack(task, PA)

    def centralized_mode(self):
        print('{} start to run centralized mode'.format(self.name))
        while True:
            for d in self.data.keys():
                now = time.time()
                self.client.publish(d, json.dumps({'time': now,
                                                   'content': self.data[d],
                                                   'RA': self.name
                                                  }))
            time.sleep(5)

    def run(self):
        self.distributed_mode()

    def listen(self):
        def start_pubsub_listener():
            for m in self.sub.listen():
                if m.get("type") == "message":
                    # latency = time.time() - float(m['data'])
                    # print('Recieved: {0}'.format(latency))
                    channel = m['channel']
                    msg = json.loads(m['data'])
                    self.message_queue.append((channel, msg))

        def start_socket_listener():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.name, 7000))
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
                            self.switch_to_centralized()
                        elif msg['type'] == 'order':
                            def dist(a, b):
                                return abs(a[0] - b[0]) + abs(a[1] - b[1])
                            duration = dist(msg['current position'], self.pos) + self.tasks[msg['task']]
                            Thread(target=self.wait_and_ack, args=(duration, msg['PA'])).start()

        Thread(target=start_pubsub_listener).start()
        Thread(target=start_socket_listener).start()

    def wait_and_ack(self, duration, PA):
        time.sleep(np.random.normal(duration, 2))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((PA, 7000))
            s.send(json.dumps({'type': 'finish ack',
                               'task': 'A'
                               }).encode())

if __name__ == "__main__":
    args = sys.argv[1:]
    if args[1] == '2':
        ResourceAgent(args[0], (int(args[2]), int(args[3])), {'A': 20, 'B': 10}).start()
    else:
        ResourceAgent(args[0], (int(args[2]), int(args[3])), {'A': 10}).start()
