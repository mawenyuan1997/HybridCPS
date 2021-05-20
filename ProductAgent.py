import sys

import redis
import time
import json
from threading import Thread
import socket

class ProductAgent(Thread):

    def __init__(self, name, tasks, data=None):
        super().__init__()
        self.name = name
        self.tasks = tasks
        self.data = data
        self.knowledge = {}
        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host='192.168.1.100', port=6379,
            decode_responses=True, encoding='utf-8'))
        self.sub = self.client.pubsub()
        self.message_queue = []
        self.sub.subscribe(self.tasks)
        self.listen_thread = None
        self.listen()

    def announce_task(self, task, current_pos):
        print('{} announce task {}'.format(self.name, task))
        now = time.time()
        self.client.publish(task, json.dumps({'time': now,
                                              'type': 'announcement',
                                              'PA name': self.name,
                                              'current position': current_pos
                                              }))

    def wait_for_bid(self, task):
        print('{} wait for {}\'s bid'.format(self.name, task))
        start = time.time()
        bids = []
        while (time.time() - start < 3):
            while self.message_queue:
                channel, msg = self.message_queue.pop(0)
                if msg['type'] == 'bid' and channel == task:
                    bids.append(msg.copy())
        return bids

    def find_best_bid(self, bids):
        earliest = 100000
        best = None
        for bid in bids:
            if bid['finish time'] < earliest:
                earliest = bid['finish time']
                best = bid
        print('{} go to {}'.format(self.name, best['position']))
        return best

    def confirm_bid(self, task, bid):
        print('{} confirm bid {}'.format(self.name, task))
        now = time.time()
        self.client.publish(task, json.dumps({'time': now,
                                              'type': 'bid confirm',
                                              'RA name': bid['RA name'],
                                              'PA name': self.name
                                              }))

    def wait_for_finish(self, task, finish_time):
        print('{} wait task {} finish'.format(self.name, task))
        start = time.time()
        while (time.time() - start < finish_time + 10):
            while self.message_queue:
                channel, msg = self.message_queue.pop(0)
                if msg['type'] == 'finish ack' and channel == task and msg['PA name'] == self.name:
                    print('{} gets finish ack'.format(self.name))
                    return True
        print('{} timeout for finish ack'.format(self.name))
        return False

    def distributed_mode(self):
        print('{} run in distributed mode'.format(self.name))
        start_time = time.time()
        current_pos = (10, 10)
        for task in self.tasks:
            bids = []
            while not bids:
                self.announce_task(task, current_pos)
                bids = self.wait_for_bid(task)
            best_bid = self.find_best_bid(bids)
            self.confirm_bid(task, best_bid)
            self.wait_for_finish(task, best_bid['finish time'])
            current_pos = best_bid['position']
        print('{} finished {}s'.format(self.name, time.time() - start_time))

    def centralized_mode(self):
        print('{} run in centralized mode'.format(self.name))


    def switch_to_centralized(self):
        self.sub.unsubscribe(self.tasks)
        self.sub.subscribe(self.data)

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('192.168.1.100', 7000))
            s.send(json.dumps({'type': 'switch to centralized request',
                               'RAs': ['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4']}).encode())
            data = s.recv(1024)
        msg = json.loads(data.decode())
        if msg['type'] == 'agree to switch':
            self.switch_to_centralized()
            self.centralized_mode()
        else:
            self.distributed_mode()

    def listen(self):
        def start_listener():
            for m in self.sub.listen():
                if m.get("type") == "message":
                    # latency = time.time() - float(m['data'])
                    # print('Recieved: {0}'.format(latency))
                    channel = m['channel']
                    msg = json.loads(m['data'])
                    if channel in self.tasks:
                        self.message_queue.append((channel, msg))
                    else:
                        if channel in self.knowledge:
                            self.knowledge[channel][msg['RA']] = msg['content']
                        else:
                            self.knowledge[channel] = {msg['RA']: msg['content']}
                        print('receive {}'.format(msg))

        self.listen_thread = Thread(target=start_listener)
        self.listen_thread.start()


if __name__ == "__main__":
    args = sys.argv[1:]
    ProductAgent(args[0], ['A', 'B'], ['position']).start()
