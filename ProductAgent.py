import sys

import redis
import time
import json
from threading import Thread
import socket
import utils

class ProductAgent(Thread):

    def __init__(self, name, addr, port, tasks, start_pos, interests=None):
        super().__init__()
        self.name = name
        self.addr = addr
        self.port = port
        self.tasks = tasks
        self.interests = interests
        self.knowledge = {}
        self.current_pos = start_pos
        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host=utils.IP['pubsub'], port=utils.PORT['pubsub'],
            decode_responses=True, encoding='utf-8'))
        self.sub = self.client.pubsub()
        self.message_queue = []
        self.A_finish = False
        self.B_finish = False
        self.sub.subscribe(self.tasks)
        self.listen_thread = None
        self.listen()

    def announce_task(self, task):
        print('{} announce task {}'.format(self.name, task))
        now = time.time()
        self.client.publish(task, json.dumps({'time': now,
                                              'type': 'announcement',
                                              'PA name': self.name,
                                              'current position': self.current_pos
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

        for task in self.tasks:
            bids = []
            while not bids:
                self.announce_task(task)
                bids = self.wait_for_bid(task)
            best_bid = self.find_best_bid(bids)
            self.confirm_bid(task, best_bid)
            self.wait_for_finish(task, best_bid['finish time'])
            self.current_pos = best_bid['position']
        print('{} finished {}s'.format(self.name, time.time() - start_time))

    def centralized_mode(self):
        print('{} run in centralized mode'.format(self.name))
        current_env = {}
        contain_all = False
        while not contain_all:
            current_env = self.knowledge.copy()
            contain_all = True
            for d in self.interests:
                if not (d in current_env and len(current_env[d]) == 4):
                    contain_all = False
                    break

        print('current env: {}'.format(current_env))
        start_time = time.time()

        def dist(a,b):
            return abs(a[0] - b[0]) + abs(a[1] - b[1])

        opt_A, opt_B = None, None
        quickest = 100000
        for ra_A in current_env['capability'].keys():
            if 'A' in current_env['capability'][ra_A]:
                for ra_B in current_env['capability'].keys():
                    if 'B' in current_env['capability'][ra_B]:
                        pos_A = current_env['position'][ra_A]
                        pos_B = current_env['position'][ra_B]
                        duration = dist(self.current_pos, pos_A) + current_env['capability'][ra_A]['A'] + dist(pos_A, pos_B) + current_env['capability'][ra_B]['B']
                        if duration < quickest:
                            quickest = duration
                            opt_A, opt_B = ra_A, ra_B
        print(opt_A, opt_B, quickest)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            A_addr, A_port = current_env['address'][opt_A][0], current_env['address'][opt_A][1]
            s.connect((A_addr, A_port))
            s.send(json.dumps({'type': 'order',
                               'task': 'A',
                               'current position': self.current_pos,
                               'PA address': (self.addr, self.port)}).encode())
        while not self.A_finish:
            time.sleep(1)
        print('A finished at {} secs'.format(time.time() - start_time))
        self.current_pos = current_env['position'][ra_A]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            B_addr, B_port = current_env['address'][opt_B][0], current_env['address'][opt_B][1]
            s.connect(B_addr, B_port)
            s.send(json.dumps({'type': 'order',
                               'task': 'B',
                               'current position': self.current_pos,
                               'PA': self.name}).encode())
        while not self.B_finish:
            time.sleep(1)
        print('{} finish in {} secs'.format(self.name, time.time() - start_time))

    def switch_to_centralized(self):
        self.sub.unsubscribe(self.tasks)
        self.sub.subscribe(self.interests)

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((utils.IP['pubsub'], utils.PORT['coordinator']))
            s.send(json.dumps({'type': 'switch to centralized request',
                               'RAs': [(utils.IP['Node1'], utils.PORT['RA1']),
                                       (utils.IP['Node2'], utils.PORT['RA2']),
                                       (utils.IP['Node3'], utils.PORT['RA3']),
                                       (utils.IP['Node4'], utils.PORT['RA4'])]}).encode())
            data = s.recv(1024)
        msg = json.loads(data.decode())
        if msg['type'] == 'agree to switch':
            self.switch_to_centralized()
            self.centralized_mode()
        else:
            self.distributed_mode()

    def listen(self):
        def start_socket_listener():
            while True:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind((self.addr, self.port))
                    s.listen()
                    conn, addr = s.accept()
                    with conn:
                        while True:
                            data = conn.recv(1024)
                            if not data:
                                break
                            msg = json.loads(data.decode())
                            if msg['type'] == 'finish ack':
                                if msg['task'] == 'A':
                                    print('A finish')
                                    self.A_finish = True
                                else:
                                    self.B_finish = True

        def start_pubsub_listener():
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

        self.listen_thread = Thread(target=start_pubsub_listener)
        self.listen_thread.start()

        Thread(target=start_socket_listener).start()



if __name__ == "__main__":
    args = sys.argv[1:]
    name = args[0]
    addr = args[1]
    port = int(args[2])
    tasks = ['A', 'B']
    interests = ['position', 'capability', 'address']
    start = (10, 10)
    ProductAgent(name, addr, port, tasks, start, interests=interests).start()
