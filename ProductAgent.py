import sys

import redis
import time
import json
from threading import Thread
import socket
import utils


class ProductAgent(Thread):

    def __init__(self, name, ip, port, config):
        super().__init__()
        self.name = name
        self.ip = ip
        self.port = port
        self.tasks = config['tasks']
        self.interests = config['interests']
        self.knowledge = {}
        self.type = config['type']
        self.current_pos = tuple(config['start position'])
        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host=utils.IP['pubsub'], port=utils.PORT['pubsub'],
            decode_responses=True, encoding='utf-8'))
        self.sub = self.client.pubsub()
        self.pubsub_queue = []
        self.message_queue = []
        self.ack_received = False
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
        # print('{} wait for {}\'s bid'.format(self.name, task))
        start = time.time()
        bids = []
        while time.time() - start < 3:
            while self.pubsub_queue:
                channel, msg = self.pubsub_queue.pop(0)
                if msg['type'] == 'bid' and channel == task:
                    bids.append(msg.copy())
        return bids

    def distance(self, a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

    # return the name of the closest resource
    def find_best_bid(self, bids):
        dist = 100000
        best = None
        for bid in bids:
            if self.distance(bid['RA location'], self.current_pos) < dist:
                dist = self.distance(bid['RA location'], self.current_pos)
                best = dict(bid)
        print('{} go to {}'.format(self.name, best['RA location']))
        return best

    # find the shortest path towards a resource
    def find_path(self, bids, dest):
        edges = []
        for bid in bids:
            for e in bid['edges']:
                edges.append((tuple(e[0]), tuple(e[1]), bid))
        visited = set()

        def dfs(x, path):
            visited.add(x)
            if x == dest:
                return
            for e in edges:
                if e[0] == x and e[1] not in visited:
                    path.append((e[2], x))
                    dfs(e[1], path)
                    if path[-1][1] == dest:
                        return
                    path = path[:-1]

        path = []
        dfs(self.current_pos, path)
        if not path:
            print('no path found')
        else:
            print([(x[0], x[1]) for x in path])
        return path

    def confirm_bid(self, task, ra_name, task_info=None):
        print('{} confirm bid {}'.format(self.name, task))
        now = time.time()
        self.client.publish(task, json.dumps({'time': now,
                                              'type': 'bid confirm',
                                              'RA name': ra_name,
                                              'PA name': self.name
                                              }))

    # wait for RA's finish ack
    def wait_for_finish(self, ra_name, finish_time):
        print('{} wait ra {} finish'.format(self.name, ra_name))
        start = time.time()
        while time.time() - start < finish_time + 10:
            while self.message_queue:
                msg = self.message_queue.pop(0)
                if msg['type'] == 'finish ack' and msg['RA name'] == ra_name and msg['PA name'] == self.name:
                    print('{} gets finish ack'.format(self.name))
                    return True
                self.message_queue.append(msg)
        print('{} timeout for finish ack'.format(self.name))
        return False

    def send_msg(self, ra_addr, msg):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            ip, port = ra_addr
            s.connect((ip, port))
            s.send(json.dumps(msg).encode())

    def distributed_mode(self):
        print('{} run in distributed mode'.format(self.name))
        start_time = time.time()
        for task in self.tasks:
            # find a bidder for task
            bids = []
            while not bids:
                self.announce_task(task)
                bids = self.wait_for_bid(task)
            best_bid = self.find_best_bid(bids)
            self.confirm_bid(task, best_bid['RA name'])

            # figure out topology
            self.announce_task('transport')
            bids = self.wait_for_bid('transport')
            ra_team = self.find_path(bids, tuple(best_bid['RA location']))

            # confirm all transport RAs on the chosen path
            for bid, pos in ra_team:
                self.confirm_bid('transport', bid['RA name'], task_info={'start': self.current_pos, 'destination': pos})

            # send control command to transport RAs one by one
            for bid, pos in ra_team:
                self.send_msg(bid['RA address'], {'type': 'order',
                                                  'task': 'transport',
                                                  'start': self.current_pos,
                                                  'destination': pos,
                                                  'PA address': (self.ip, self.port),
                                                  'PA name': self.name
                                                  })
                self.wait_for_finish(bid['RA name'], (self.distance(self.current_pos, pos) / bid['velocity']))
                self.current_pos = pos.copy()

            # send control command to processing RA
            self.send_msg(best_bid['RA address'], {'type': 'order',
                                                   'task': task,
                                                   'PA address': (self.ip, self.port),
                                                   'PA name': self.name
                                                   })

            self.wait_for_finish(best_bid['RA name'], best_bid['processing time'])
            self.current_pos = tuple(best_bid['RA location'])
        print('{} finished {}s'.format(self.name, time.time() - start_time))

    def centralized_mode(self):
        print('{} run in centralized mode'.format(self.name))

        #
        #
        # print(opt_A, opt_B, quickest)
        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #     A_addr, A_port = current_env['address'][opt_A][0], current_env['address'][opt_A][1]
        #     s.connect((A_addr, A_port))
        #     s.send(json.dumps({'type': 'order',
        #                        'task': 'A',
        #                        'current position': self.current_pos,
        #                        'PA address': (self.ip, self.port)}).encode())
        # while not self.A_finish:
        #     time.sleep(1)
        # print('A finished at {} secs'.format(time.time() - start_time))
        # self.current_pos = current_env['position'][opt_A]
        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #     B_addr, B_port = current_env['address'][opt_B][0], current_env['address'][opt_B][1]
        #     s.connect((B_addr, B_port))
        #     s.send(json.dumps({'type': 'order',
        #                        'task': 'B',
        #                        'current position': self.current_pos,
        #                        'PA address': (self.ip, self.port)}).encode())
        # while not self.B_finish:
        #     time.sleep(1)
        # print('{} finish in {} secs'.format(self.name, time.time() - start_time))

    def switch_to_centralized(self):
        self.sub.unsubscribe(self.tasks)
        self.sub.subscribe(self.interests)

    def run(self):
        if self.type == 'rush order':
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
        elif self.type == 'normal order':
            self.distributed_mode()

    def listen(self):
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

        def start_pubsub_listener():
            for m in self.sub.listen():
                if m.get("type") == "message":
                    # latency = time.time() - float(m['data'])
                    # print('Recieved: {0}'.format(latency))
                    channel = m['channel']
                    msg = json.loads(m['data'])
                    self.pubsub_queue.append((channel, msg))
                    # else:
                    #     if channel in self.knowledge:
                    #         self.knowledge[channel][msg['RA name']] = msg['content']
                    #     else:
                    #         self.knowledge[channel] = {msg['RA name']: msg['content']}

        self.listen_thread = Thread(target=start_pubsub_listener)
        self.listen_thread.start()

        Thread(target=start_socket_listener).start()


if __name__ == "__main__":
    args = sys.argv[1:]
    name = args[0]
    addr = args[1]
    port = int(args[2])
    config_file = args[3]

    f = open(config_file, )
    config = json.load(f)
    f.close()
    ProductAgent(name, addr, port, config).start()
