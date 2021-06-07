import redis
import time
import json
import threading
import numpy as np
from threading import Thread
import sys
import socket
import utils
import os


class ResourceAgent(Thread):

    def __init__(self, name, addr, port, tasks, data):
        super().__init__()
        self.name = name
        self.ip = addr
        self.port = port
        self.tasks = tasks
        self.data = data

        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host=utils.IP['pubsub'], port=utils.PORT['pubsub'],
            decode_responses=True, encoding='utf-8'))

        self.sub = self.client.pubsub()
        self.sub.subscribe(self.tasks)

        self.current_mode = 'distributed'

        self.pubsub_queue = []
        self.message_queue = []
        self.listen()

    def wait_for_task(self):
        print('{} wait for task'.format(self.name))
        while True:
            while self.pubsub_queue:
                channel, msg = self.pubsub_queue.pop(0)
                if msg['type'] == 'announcement':
                    return channel, msg['PA name']
            time.sleep(1)

    def send_bid(self, task):
        print('{} send bid to task {}'.format(self.name, task))
        bid = {'type': 'bid',
               'RA name': self.name,
               'RA location': self.data['location'],
               'RA address': (self.ip, self.port)
               }
        if 'Robot' in self.name or 'Buffer' in self.name:
            bid['edges'] = self.data['edges']
            bid['velocity'] = self.data['velocity']
        else:
            bid['processing time'] = self.data['capability'][task]
        self.client.publish(task, json.dumps(bid))

    def wait_for_confirm(self, task, pa_name):
        print('{} wait for task {} confirm from {}'.format(self.name, task, pa_name))
        start = time.time()
        while time.time() - start < 5:
            while self.pubsub_queue:
                channel, msg = self.pubsub_queue.pop(0)
                if msg['type'] == 'bid confirm' and channel == task and msg['PA name'] == pa_name:
                    if msg['RA name'] == self.name:
                        return True
        print('{} timeout wait for confirm'.format(self.name))
        return False

    def distance(self, a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

    def get_duration(self, task, order_msg):
        if order_msg['task'] == 'transport':
            return (self.distance(self.data['location'], order_msg['start'])
                    + self.distance(order_msg['start'], order_msg['destination'])) / self.data['velocity']
        else:
            return self.data['capability'][task]

    def execute_task_and_ack(self, task, pa_addr, duration):
        print('wait for {}'.format(duration))
        time.sleep(np.random.normal(duration, 2))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((pa_addr[0], pa_addr[1]))
            s.send(json.dumps({'type': 'finish ack',
                               'task': task,
                               'RA name': self.name
                               }).encode())

    def switch_to_centralized(self):
        self.sub.unsubscribe(self.tasks.keys())
        self.current_mode = 'centralized'

    def distributed_mode(self):
        # print('{} start to run distributed mode'.format(self.name))
        task, pa_name = self.wait_for_task()  # blocking
        self.send_bid(task)
        bid_accept = self.wait_for_confirm(task, pa_name)
        if bid_accept:
            # wait for PA's command and do the task
            start = time.time()
            while time.time() - start < 10:
                while self.message_queue:
                    msg = self.message_queue.pop(0)
                    if msg['type'] == 'order' and msg['PA name'] == pa_name:
                        duration = self.get_duration(task, msg)
                        self.execute_task_and_ack(task, msg['PA address'], duration)
                    else:
                        self.message_queue.append(msg)

    def centralized_mode(self):
        for d in self.data.keys():
            now = time.time()
            self.client.publish(d, json.dumps({'time': now,
                                               'content': self.data[d],
                                               'RA': self.name
                                               }))
        time.sleep(5)

    def run(self):
        while True:
            print('{} current mode: {}'.format(self.name, self.current_mode))
            while self.current_mode == 'distributed':
                self.distributed_mode()
            print('{} current mode: {}'.format(self.name, self.current_mode))
            while self.current_mode == 'centralized':
                self.centralized_mode()

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
            while True:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind((self.ip, self.port))
                    s.listen()
                    conn, addr = s.accept()
                    with conn:
                        while True:
                            data = conn.recv(1024)
                            if not data:
                                break
                            msg = json.loads(data.decode())
                            self.message_queue.append(msg)
                            if msg['type'] == 'switch to centralized request':
                                print('{} receive switch request'.format(self.name))
                                self.switch_to_centralized()

        Thread(target=start_pubsub_listener).start()
        Thread(target=start_socket_listener).start()


if __name__ == "__main__":
    args = sys.argv[1:]
    name = args[0]
    addr, port = args[1], int(args[2])
    config_file = args[3]

    f = open(config_file, )
    config = json.load(f)
    tasks = config['tasks']
    data = config['data']
    f.close()
    if "Buffer" in name:
        edges = []
        for p in data['unloading point']:
            edges.append((data['location'], p))
            edges.append((p, data['location']))
        data['edges'] = edges
    elif "Robot" in name:
        edges = []
        for p in data['unloading point']:
            for q in data['unloading point']:
                if p != q:
                    edges.append((q, p))
                    edges.append((p, q))
        data['edges'] = edges

    ResourceAgent(name, addr, port, tasks, data).start()
