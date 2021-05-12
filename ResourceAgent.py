import redis
import time
import json
import threading
import numpy as np
from threading import Thread
import sys


class ResourceAgent(Thread):

    def __init__(self, name, tasks=['task1', 'task2'], data=[str(i) for i in range(10)]):
        super().__init__()
        self.name = name
        self.tasks = tasks
        self.data = data
        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host='192.168.1.100', port=6379,
            decode_responses=True, encoding='utf-8'))

        self.sub = self.client.pubsub()
        for task in self.tasks:
            self.sub.subscribe(task)

        print(self.name + ' starts at ' + str(time.time()))
        self.need_transition = True

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

    def transition(self):
        for task in self.tasks:
            self.sub.unsubscribe(task)
        for d in self.data:
            now = time.time()
            self.client.publish(d, str({'time': now,
                                        'RA name': self.name,
                                        'finish time': np.random.normal(10, 2)
                                        }))
        print(self.name + ' ends at ' + str(time.time()))

    def run(self):
        while True:
            if self.need_transition:
                self.transition()
                break
            task, PA = self.wait_for_task()
            self.send_bid(task)
            bid_accept = self.wait_for_confirm(task, PA)
            if bid_accept:
                self.send_command_and_wait()
                self.send_finish_ack(PA)

if __name__ == "__main__":
    args = sys.argv[1:]
    ResourceAgent(args[0]).start()
