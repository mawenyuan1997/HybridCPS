import redis
import time
import json
import threading
import numpy as np
from threading import Thread
import sys
import socket
import utils
from utils import distance
import os
import select


class ResourceAgent(Thread):

    def __init__(self, name, ip, port, tasks, data, start_mode, resource_addr):
        super().__init__()
        self.name = name
        self.ip = ip
        self.port = port
        self.tasks = tasks
        self.data = data
        self.resource_addr = resource_addr

        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host=utils.IP['pubsub'], port=utils.PORT['pubsub'],
            decode_responses=True, encoding='utf-8'))

        self.sub = self.client.pubsub()
        self.current_mode = start_mode
        if self.current_mode == 'centralized':
            self.need_publish_data = True
        else:
            self.need_publish_data = False
            self.sub.subscribe(self.tasks)
        self.data_publish_thread = None
        self.need_switch = False
        self.pubsub_queue = []
        self.message_queue = []
        self.listen()
        # Thread(target=self.get_data).start()

    # for distributed mode
    def wait_for_task(self):
        # print('{} wait for task'.format(self.name))
        while True:
            while self.pubsub_queue:
                channel, msg = self.pubsub_queue.pop(0)
                if msg['type'] == 'announcement':
                    return channel, msg['PA name']
                self.pubsub_queue.append((channel, msg))
            time.sleep(1)

    # for distributed mode
    def send_bid(self, task):
        # print('{} send bid to task {}'.format(self.name, task))
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

    # for distributed mode
    def wait_for_confirm(self, task, pa_name):
        start = time.time()
        while time.time() - start < utils.BID_CONFIRM_TIMEOUT:
            while self.pubsub_queue and time.time() - start < utils.BID_CONFIRM_TIMEOUT:
                channel, msg = self.pubsub_queue.pop(0)
                if (msg['type'] == 'bid confirm' and channel == task and msg['PA name'] == pa_name
                        and msg['RA name'] == self.name):
                    print('{} receive task {} confirm from {}'.format(self.name, task, pa_name))
                    return True
                else:
                    self.pubsub_queue.append((channel, msg))

        # print('{} timeout wait for confirm'.format(self.name))  # TODO: add PA sending reject msg maybe
        return False

    def get_duration(self, task, order_msg):
        if order_msg['task'] == 'transport':
            return (distance(self.data['location'], order_msg['start'])
                    + distance(order_msg['start'], order_msg['destination'])) / self.data['velocity']
        else:
            return self.data['capability'][task]

    def execute_task_and_ack(self, task, msg, duration):
        print('{} wait for {}'.format(self.name, duration))
        # send order command to resource
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.resource_addr[0], self.resource_addr[1]))
            s.send(json.dumps(msg).encode() + b'\n')

            # time.sleep(abs(np.random.normal(duration, utils.STD_ERR)))   # delay some time to simulate machine processing

            timeout = 10
            start = time.time()
            # TODO: add timeout
            received = s.recv(1000)

            if received:
                ack_msg = json.loads(received.decode())
                if ack_msg['type'] == 'finish ack':
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                        ss.connect((msg['PA address'][0], msg['PA address'][1]))
                        ss.send(json.dumps({'type': 'finish ack',
                                            'task': task,
                                            'RA name': self.name
                                            }).encode())
            else:
                print('{} timeout for finish ack'.format(self.name))

    def distributed_mode(self):
        # print('{} start to run distributed mode'.format(self.name))
        task, pa_name = self.wait_for_task()  # blocking
        self.send_bid(task)
        bid_accept = self.wait_for_confirm(task, pa_name)
        if bid_accept:
            # wait for PA's command and do the task
            start = time.time()
            while time.time() - start < utils.COMMAND_ORDER_TIMEOUT:
                while self.message_queue:
                    msg = self.message_queue.pop(0)
                    if msg['type'] == 'order' and msg['PA name'] == pa_name:
                        duration = self.get_duration(task, msg)
                        self.execute_task_and_ack(task, msg, duration)
                        break
                    else:
                        self.message_queue.append(msg)
            if time.time() - start > utils.COMMAND_ORDER_TIMEOUT:
                print('{} timeout waiting for command from {}'.format(self.name, pa_name))

    def centralized_mode(self):
        def publish_data():
            while self.need_publish_data:
                for d in self.data.keys():
                    now = time.time()
                    self.client.publish(d, json.dumps({'time': now,
                                                       'content': self.data[d],
                                                       'RA name': self.name
                                                       }))
                    time.sleep(1)
                time.sleep(5)

        if not self.data_publish_thread:
            self.data_publish_thread = Thread(target=publish_data)
            self.data_publish_thread.start()

        while self.message_queue:
            msg = self.message_queue.pop(0)
            if msg['type'] == 'order':
                duration = self.get_duration(msg['task'], msg)
                self.execute_task_and_ack(msg['task'], msg, duration)
                break
            else:
                self.message_queue.append(msg)

    def run(self):
        while True:
            if self.current_mode == 'distributed':
                self.distributed_mode()
            else:
                self.centralized_mode()
            if self.need_switch:
                if self.current_mode == 'distributed':
                    self.current_mode = 'centralized'
                    self.sub.unsubscribe(self.tasks)
                    self.need_publish_data = True
                else:
                    self.current_mode = 'distributed'
                    self.sub.subscribe(self.tasks)
                    self.need_publish_data = False
                self.need_switch = False

    def listen(self):
        def start_pubsub_listener():
            while True:
                for m in self.sub.listen():
                    if m.get("type") == "message":
                        # latency = time.time() - float(m['data'])
                        # print('Recieved: {0}'.format(latency))
                        channel = m['channel']
                        msg = json.loads(m['data'])
                        self.pubsub_queue.append((channel, msg))

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
                            print(msg)
                            if msg['type'] == 'switch request':
                                self.need_switch = True
                            else:
                                self.message_queue.append(msg)

        Thread(target=start_pubsub_listener).start()
        Thread(target=start_socket_listener).start()

    # obtain data from the resource
    def get_data(self, period=3):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.resource_addr[0], self.resource_addr[1]))
            while True:
                s.send(json.dumps({'type': 'data request'
                                   }).encode() + b'\n')
                data = s.recv(1024)
                msg = json.loads(data.decode()[:-1])
                # TODO update resource data
                time.sleep(period)


if __name__ == "__main__":
    args = sys.argv[1:]
    name = args[0]
    ip, port = args[1], int(args[2])
    config_file = args[3]
    start_mode = args[4]

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
                    edges.append((p, q))
        data['edges'] = edges
    data['RA address'] = (ip, port)
    resource_addr = config['resource address']
    ResourceAgent(name, ip, port, tasks, data, start_mode, resource_addr).start()
