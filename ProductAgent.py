import redis
import time
import json
from threading import Thread


class ProductAgent(Thread):

    def __init__(self, name, tasks, data=None):
        super().__init__()
        self.name = name
        self.tasks = tasks
        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host='192.168.1.100', port=6379,
            decode_responses=True, encoding='utf-8'))

    def announce_task(self, task_name):
        now = time.time()
        print('Sending {0}'.format(now))
        self.client.publish(task_name, str({'time': now,
                                            'type': 'announcement',
                                            'PA name': self.name
                                            }))
        sub = self.client.pubsub()
        sub.subscribe(task_name)
        return sub

    def wait_for_bid(self, channel):
        start = time.time()
        bids = []
        while (time.time() - start < 10):
            for m in channel.listen():
                if m.get("type") == "message":
                    # latency = time.time() - float(m['data'])
                    # print('Recieved: {0}'.format(latency))
                    msg = json.loads(m['data'])
                    if msg['type'] == 'bid':
                        bids.append(msg.copy())
        return bids

    def find_best_bid(self, bids):
        earliest = 100000
        best = None
        for bid in bids:
            if bid['finish time'] < earliest:
                earliest = bid['finish time']
                best = bid
        return best

    def confirm_bid(self, task_name, bid):
        now = time.time()
        self.client.publish(task_name, str({'time': now,
                                            'type': 'bid confirm',
                                            'RA name': bid['RA name'],
                                            'PA name': self.name
                                            }))

    def wait_for_finish(self, task, channel):
        while True:
            for m in channel.listen():
                if m.get("type") == "message":
                    # latency = time.time() - float(m['data'])
                    # print('Recieved: {0}'.format(latency))
                    msg = json.loads(m['data'])
                    if msg['type'] == 'finish ack' and msg['task'] == task:
                        return

    def run(self):
        start_time = time.time()
        for task in ['A', 'B']:
            for i in range(self.tasks[task]):
                channel = self.announce_task(task)
                bids = self.wait_for_bid(channel)
                best_bid = self.find_best_bid(bids)
                self.confirm_bid(task, best_bid)
                self.wait_for_finish(task)
        print('{} finished {}s'.format(self.name, time.time() - start_time))
