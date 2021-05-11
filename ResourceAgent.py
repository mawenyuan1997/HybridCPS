import redis
import time
import json
import threading
import numpy as np

class ResourceAgent(object):

    def __init__(self, name, tasks, data=None):
        self.name = name
        self.tasks = tasks
        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host='10.0.0.1', port=6379,
            decode_responses=True, encoding='utf-8'))

        self.sub = self.client.pubsub()
        for task in self.tasks:
            self.sub.subscribe(task)

    def send_command_and_wait(self):
        time.sleep(10)

    def wait_for_task(self):

        while True:
            for m in self.sub.listen():
                if m.get("type") == "message":
                    # latency = time.time() - float(m['data'])
                    # print('Recieved: {0}'.format(latency))
                    msg = json.loads(m['data'])
                    if msg['type'] == 'announcement':
                        return m['channel'], msg['PA name']

    def send_bid(self, task):
        now = time.time()
        self.client.publish(task, str({'time': now,
                                       'type': 'bid',
                                       'RA name': self.name,
                                       'finish time': np.random.normal(10, 2)
                                        }))

    def wait_for_confirm(self, task, PA):
        start = time.time()
        while time.time() - start < 10:
            for m in self.sub.listen():
                if m.get("type") == "message":
                    # latency = time.time() - float(m['data'])
                    # print('Recieved: {0}'.format(latency))
                    msg = json.loads(m['data'])
                    if msg['type'] == 'bid confirm' and m['channel'] == task and msg['PA name'] == PA:
                        return msg['RA name'] == self.name
        return False

    def send_finish_ack(self, PA):
        print('done')

    def start(self):
        while True:
            task, PA = self.wait_for_task()
            self.send_bid(task)
            bid_accept = self.wait_for_confirm(task, PA)
            if bid_accept:
                self.send_command_and_wait()
                self.send_finish_ack(PA)


