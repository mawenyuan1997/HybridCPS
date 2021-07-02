import sys

import redis
import time
import json
from threading import Thread
import socket
import utils
from utils import send_msg
from utils import distance


class ProductAgent(Thread):

    def __init__(self, name, ip, port, config, start_mode):
        super().__init__()
        self.name = name
        self.ip = ip
        self.port = port
        self.tasks = config['tasks']
        self.next_task_index = 0
        self.interests = config['interests']
        self.schedule_mode, self.dispatch_mode = start_mode
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
        self.need_switch = False
        self.sub.subscribe(self.tasks)
        self.sub.subscribe(['transport', 'dispatching'])  # PA needs transport tasks in addition to processing tasks
        self.listen_thread = None
        self.listen()

    # for distributed mode
    def announce_task(self, task):
        print('{} announce task {}'.format(self.name, task))
        now = time.time()
        self.client.publish(task, json.dumps({'time': now,
                                              'type': 'announcement',
                                              'PA name': self.name,
                                              'current position': self.current_pos
                                              }))

    # for distributed mode
    def wait_for_bid(self, task, timeout=3):
        # print('{} wait for {}\'s bid'.format(self.name, task))
        start = time.time()
        bids = []
        while time.time() - start < timeout:
            while self.pubsub_queue and time.time() - start < timeout:
                channel, msg = self.pubsub_queue.pop(0)
                if msg['type'] == 'bid' and channel == task:
                    bids.append(msg.copy())
                else:
                    self.pubsub_queue.append((channel, msg))
        return bids

    # for distributed mode, return the name of the closest resource
    def find_best_bid(self, bids):
        dist = 100000
        best = None
        for bid in bids:
            if distance(bid['RA location'], self.current_pos) < dist:
                dist = distance(bid['RA location'], self.current_pos)
                best = dict(bid)
        print('{} go to {}'.format(self.name, best['RA location']))
        return best

    # for distributed mode, find the shortest path towards a resource
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
        print(self.current_pos)
        dfs(self.current_pos, path)
        if not path:
            print('no path found')
        else:
            print('find path {}'.format([x[1] for x in path]))
        return path

    # for distributed mode
    def confirm_bid(self, task, ra_name, task_info=None):
        # print('{} confirm bid {}'.format(self.name, task))
        now = time.time()
        self.client.publish(task, json.dumps({'time': now,
                                              'type': 'bid confirm',
                                              'RA name': ra_name,
                                              'PA name': self.name
                                              }))

    # wait for RA's finish ack
    def wait_for_finish(self, ra_name, finish_time, timeout=10):
        print('{} wait ra {} finish'.format(self.name, ra_name))
        start = time.time()
        while time.time() - start < finish_time + timeout:
            while self.message_queue and time.time() - start < finish_time + timeout:
                msg = self.message_queue.pop(0)
                if msg['type'] == 'finish ack' and msg['RA name'] == ra_name:
                    print('{} gets finish ack'.format(self.name))
                    return True
                self.message_queue.append(msg)
        print('{} timeout for finish ack'.format(self.name))
        return False

    def distributed_mode(self):
        print('{} run in distributed mode'.format(self.name))
        start_time = time.time()
        while self.next_task_index < len(self.tasks):
            task = self.tasks[self.next_task_index]
            # check if needs to switch mode
            if self.need_switch:
                self.switch_to_centralized()
                return
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
                send_msg(bid['RA address'], {'type': 'order',
                                             'task': 'transport',
                                             'start': self.current_pos,
                                             'destination': pos,
                                             'PA address': (self.ip, self.port),
                                             'PA name': self.name
                                             })
                if_finished = self.wait_for_finish(bid['RA name'],
                                                   (distance(self.current_pos, pos) / bid['velocity']))
                # TODO handle timeout
                self.current_pos = pos

            # send control command to processing RA
            send_msg(best_bid['RA address'], {'type': 'order',
                                              'task': task,
                                              'PA address': (self.ip, self.port),
                                              'PA name': self.name
                                              })

            if_finished = self.wait_for_finish(best_bid['RA name'], best_bid['processing time'])
            # TODO handle timeout
            self.current_pos = tuple(best_bid['RA location'])
            self.next_task_index += 1

        print('{} finished in {}s'.format(self.name, time.time() - start_time))

    def wait_for_response(self, channel, msg_type, timeout):
        start = time.time()
        while time.time() - start < timeout:
            while self.pubsub_queue and time.time() - start < timeout:
                ch, msg = self.pubsub_queue.pop(0)
                if ch == channel and msg['type'] == msg_type:
                    print('{} gets {}'.format(self.name, msg_type))
                    return msg
                self.pubsub_queue.append((ch, msg))
        print('{} timeout for {}'.format(self.name, msg_type))
        return {}

    def centralized_mode(self):
        print('{} run in centralized mode'.format(self.name))
        start_time = time.time()
        while self.next_task_index < len(self.tasks):
            task = self.tasks[self.next_task_index]
            # check if needs to switch mode
            if self.need_switch:
                self.switch_to_distributed()
                return
            msg = {'type': 'plan request',
                   'start': self.current_pos,
                   'task': task,
                   'PA address': (self.ip, self.port)}
            self.client.publish('dispatching', json.dumps(msg))

            print('{} ask for central controller plan'.format(self.name))

            if self.dispatch_mode == 'distributed':
                plan = self.wait_for_response('dispatching', 'plan', 10)
                print('{} get plan {}'.format(self.name, plan))

                for pos, ra_addr, ra_name in plan['path']:
                    send_msg(ra_addr, {'type': 'order',
                                       'task': 'transport',
                                       'start': self.current_pos,
                                       'destination': pos,
                                       'PA address': (self.ip, self.port),
                                       'PA name': self.name
                                       })
                    self.wait_for_finish(ra_name, 100)
                    self.current_pos = tuple(pos)

                # send control command to processing RA
                ra_addr, ra_name = plan['processing machine']
                send_msg(ra_addr, {'type': 'order',
                                   'task': self.tasks[self.next_task_index],
                                   'PA address': (self.ip, self.port),
                                   'PA name': self.name
                                   })

                self.wait_for_finish(ra_name, 100)
            else:
                pass  # TODO wait for central controller's finish ack

            self.next_task_index += 1

            # suppose task 1 need distributed mode
            # if self.next_task_index == 1:
            #     print('PA send switch request')
            #     send_msg((utils.IP['coordinator'], utils.PORT['coordinator']), {'type': 'need switch'})
            #     time.sleep(3)
        print('{} finished in {}s'.format(self.name, time.time() - start_time))

    def switch_to_centralized(self):
        self.sub.unsubscribe(self.tasks)
        self.need_switch = False
        self.centralized_mode()

    def switch_to_distributed(self):
        self.need_switch = False
        self.distributed_mode()

    def run(self):
        if self.schedule_mode == 'centralized':
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
                            if msg['type'] == 'switch request':
                                self.need_switch = True

        def start_pubsub_listener():
            for m in self.sub.listen():
                if m.get("type") == "message":
                    channel = m['channel']
                    msg = json.loads(m['data'])
                    self.pubsub_queue.append((channel, msg))

        self.listen_thread = Thread(target=start_pubsub_listener)
        self.listen_thread.start()

        Thread(target=start_socket_listener).start()


if __name__ == "__main__":
    args = sys.argv[1:]
    name = args[0]
    addr = args[1]
    port = int(args[2])
    config_file = args[3]
    start_mode = (args[4], args[5])

    f = open(config_file, )
    config = json.load(f)
    f.close()
    ProductAgent(name, addr, port, config, start_mode).start()
