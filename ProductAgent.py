import sys

import redis
import time
import json
from threading import Thread
import socket
import utils


class ProductAgent(Thread):

    def __init__(self, name, ip, port, config, start_mode):
        super().__init__()
        self.name = name
        self.ip = ip
        self.port = port
        self.tasks = config['tasks']
        self.interests = config['interests']
        self.current_mode = start_mode
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
        self.sub.subscribe("transport")  # PA needs transport tasks in addition to processing tasks
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
                    path.append((e[2], e[1]))
                    dfs(e[1], path)
                    if path[-1][1] == dest:
                        return
                    path.pop(-1)

        path = []
        dfs(self.current_pos, path)
        if not path:
            print('no path found')
        else:
            print([x[1] for x in path])
        return path

    def confirm_bid(self, task, ra_name, task_info=None):
        # print('{} confirm bid {}'.format(self.name, task))
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
                if msg['type'] == 'finish ack' and msg['RA name'] == ra_name:
                    print('{} gets finish ack'.format(self.name))
                    return True
                self.message_queue.append(msg)
        print('{} timeout for finish ack'.format(self.name))
        return False

    def send_msg(self, addr, msg):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            ip, port = addr
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
                self.current_pos = pos

            # send control command to processing RA
            self.send_msg(best_bid['RA address'], {'type': 'order',
                                                   'task': task,
                                                   'PA address': (self.ip, self.port),
                                                   'PA name': self.name
                                                   })

            self.wait_for_finish(best_bid['RA name'], best_bid['processing time'])
            self.current_pos = tuple(best_bid['RA location'])
        print('{} finished in {}s'.format(self.name, time.time() - start_time))

    def wait_for_response(self, msg_type, timeout):
        start = time.time()
        while time.time() - start < timeout:
            while self.message_queue:
                msg = self.message_queue.pop(0)
                if msg['type'] == msg_type:
                    print('{} gets {}'.format(self.name, msg_type))
                    return msg
                self.message_queue.append(msg)
        print('{} timeout for {}'.format(self.name, msg_type))
        return {}

    def centralized_mode(self):
        print('{} run in centralized mode'.format(self.name))
        start_time = time.time()
        msg = {'type': 'plan request',
               'start': self.current_pos,
               'task': self.tasks[0],
               'PA address': (self.ip, self.port)}
        self.send_msg((utils.IP['central controller'], utils.PORT['central controller']), msg)

        print('{} ask for central controller plan'.format(self.name))

        plan = self.wait_for_response('plan', 10)

        for pos, ra_addr, ra_name in plan['path']:
            self.send_msg(ra_addr, {'type': 'order',
                                    'task': 'transport',
                                    'start': self.current_pos,
                                    'destination': pos,
                                    'PA address': (self.ip, self.port),
                                    'PA name': self.name
                                    })
            self.wait_for_finish(ra_name, 100)
            self.current_pos = pos

        # send control command to processing RA
        ra_addr, ra_name = plan['processing machine']
        self.send_msg(ra_addr, {'type': 'order',
                                'task': self.tasks[0],
                                'PA address': (self.ip, self.port),
                                'PA name': self.name
                                })

        self.wait_for_finish(ra_name, 100)
        print('{} finished in {}s'.format(self.name, time.time() - start_time))

    def switch_to_centralized(self):
        self.sub.unsubscribe(self.tasks)
        self.centralized_mode()

    def switch_to_distributed(self):
        self.distributed_mode()

    def run(self):
        # if self.type == 'rush order':
        #     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #         s.connect((utils.IP['pubsub'], utils.PORT['coordinator']))
        #         s.send(json.dumps({'type': 'switch to centralized request',
        #                            'RAs': [(utils.IP['Node1'], utils.PORT['RA1']),
        #                                    (utils.IP['Node2'], utils.PORT['RA2']),
        #                                    (utils.IP['Node3'], utils.PORT['RA3']),
        #                                    (utils.IP['Node4'], utils.PORT['RA4'])]}).encode())
        #         data = s.recv(1024)
        #     msg = json.loads(data.decode())
        #     if msg['type'] == 'agree to switch':
        #         self.switch_to_centralized()
        #         self.centralized_mode()
        #     else:
        #         self.distributed_mode()
        # elif self.type == 'normal order':
        #     self.distributed_mode()
        if self.current_mode == 'centralized':
            self.centralized_mode()
        else:
            self.distributed_mode()

    def listen(self):
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
    start_mode = args[4]

    f = open(config_file, )
    config = json.load(f)
    f.close()
    ProductAgent(name, addr, port, config, start_mode).start()
