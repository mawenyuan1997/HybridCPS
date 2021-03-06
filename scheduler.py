import sys

import redis
import time
import json
from threading import Thread
import socket

import utils
from utils import send_msg

class Point(object):

    def __init__(self, pos, capability=None):
        self.position = pos
        self.capability = capability
        self.neighbors = []

    def add_neighbor(self, point):
        self.neighbors.append(point)

    def __eq__(self, another):
        return hasattr(another, 'position') and self.position == another.position

    def __hash__(self):
        return hash(self.position)


class Scheduler(Thread):

    def __init__(self, ip, port):
        super().__init__()
        self.ip = ip
        self.port = port
        self.client = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host=utils.IP['pubsub'], port=utils.PORT['pubsub'],
            decode_responses=True, encoding='utf-8'))
        self.sub = self.client.pubsub()
        self.interests = ['location', 'capability', 'edges', 'velocity', 'RA address', 'unloading point']
        self.sub.subscribe(self.interests)

        self.knowledge = {}
        self.optimized_plan = {}
        self.message_queue = []
        self.listen()

    def run(self):
        time.sleep(13)
        current_env = self.knowledge.copy()
        while not current_env:
            time.sleep(1)
            current_env = self.knowledge.copy()

        # find optimal path for every (source, task) pair
        for ra_name, points in current_env['unloading point'].items():
            for pos in points:
                for task in ['S1', 'S2', 'S3', 'S4', 'S5', 'S6']:
                    self.optimize(Point(tuple(pos)), task, current_env)

    # use Dijkstra to compute shortest path that will finish specific task starting from source
    # To represent processing time, add an artificial point (entrance) and an edge
    def optimize(self, source, task, current_env):
        # build graph
        machine_map = {}
        vertex = set()
        velocity = {}
        process_ra_info = {}  # processing edge => (ra_address, ra_name)
        # add processing edge
        for ra_name in current_env['location'].keys():
            if 'Machine' in ra_name:
                pos = tuple(current_env['location'][ra_name])
                cap = current_env['capability'][ra_name]
                entrance = Point((-pos[0], -pos[1]), capability=cap)  # entrance is the negative of machine coordinate
                exit = Point(pos, capability=cap)               # exit is the machine coordinate
                vertex.add(entrance)
                vertex.add(exit)
                entrance.add_neighbor(exit)
                process_ra_info[(entrance, exit)] = (current_env['RA address'][ra_name], ra_name)
                machine_map[pos] = (entrance, exit)

                if source == exit and task in cap: # no need to move
                    self.optimized_plan[(source.position, task)] = {'type': 'plan',
                                                                    'path': [],
                                                                    'processing machine': process_ra_info[(entrance, exit)]}
                    return
        ra_map = {}  # (x, y) => Point
        # add transport edges
        for ra_name in current_env['location'].keys():
            if 'Robot' in ra_name or 'Buffer' in ra_name:
                edges = current_env['edges'][ra_name]
                for begin, end in edges:
                    begin, end = tuple(begin), tuple(end)
                    # find begin/end point, create and add them to map for first time
                    if begin in machine_map:  # edge from an exit of a processing edge
                        begin_point = machine_map[begin][1]
                    elif begin not in ra_map:
                        begin_point = Point(begin)
                        ra_map[begin] = begin_point
                        vertex.add(begin_point)
                    else:
                        begin_point = ra_map[begin]

                    if end in machine_map:  # edge to an entrance of a processing edge
                        end_point = machine_map[end][0]
                    elif end not in ra_map:
                        end_point = Point(end)
                        ra_map[end] = end_point
                        vertex.add(end_point)
                    else:
                        end_point = ra_map[end]

                    begin_point.add_neighbor(end_point)
                    velocity[(begin_point, end_point)] = current_env['velocity'][ra_name]
                    process_ra_info[(begin_point, end_point)] = (current_env['RA address'][ra_name], ra_name)

        # shortest path
        dist, prev = {}, {}
        Q = set()
        for v in vertex:
            dist[v] = utils.INF
            prev[v] = None
            Q.add(v)
        dist[source] = 0
        destination = set()
        while Q:
            min_u = None
            min_dist = utils.INF
            for u in Q:
                if dist[u] < min_dist:
                    min_dist = dist[u]
                    min_u = u
            if not min_u:
                break
            Q.remove(min_u)
            for v in min_u.neighbors:
                length = utils.INF
                if v.position[0] == - (min_u.position[0]) and v.position[1] == - (min_u.position[1]):  # machine processing edge
                    if task in v.capability:
                        length = v.capability[task]
                        destination.add(v)
                else:
                    length = utils.distance(min_u.position, v.position) / velocity[(min_u, v)]
                alt = dist[min_u] + length
                if alt < dist[v]:
                    dist[v] = alt
                    prev[v] = min_u
        # find closest machine
        target = None
        min_dist = utils.INF
        for x in destination:
            if dist[x] < min_dist:
                min_dist = dist[x]
                target = x
        path = []
        u = target
        while u:
            path.insert(0, u)
            u = prev[u]
        # complete path includes transport ra info
        complete_path = []
        for i in range(1, len(path)):
            ra_addr, ra_name = process_ra_info[(path[i-1], path[i])]
            complete_path.append(((abs(path[i].position[0]), abs(path[i].position[1])), ra_addr, ra_name))

        if not complete_path:
            return               # TODO no path
        _, addr, name = complete_path[-1]
        complete_path = complete_path[:-1]
        self.optimized_plan[(source.position, task)] = {'type': 'plan',
                                                        'path': complete_path,
                                                        'processing machine': (addr, name)}

    def listen(self):
        def start_pubsub_listener():
            for m in self.sub.listen():
                if m.get("type") == "message":
                    # latency = time.time() - float(m['data'])
                    # print('Recieved: {0}'.format(latency))
                    channel = m['channel']
                    msg = json.loads(m['data'])
                    if channel not in self.knowledge:
                        self.knowledge[channel] = {msg['RA name']: msg['content']}
                    else:
                        self.knowledge[channel][msg['RA name']] = msg['content']

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
                            if msg['type'] == 'plan request':
                                print('CC receive plan request {}, {}'.format(msg['start'], msg['task']))
                                coord_addr = utils.IP['coordinator'], utils.PORT['coordinator']
                                # sample response
                                # {'type': 'plan',
                                # 'path': [[[40, 68], ['127.0.0.1', 7034], 'RobotM12'],
                                #          [[40, 72], ['127.0.0.1', 7000], 'BufferB1'],
                                #          [[30, 100], ['127.0.0.1', 7028], 'RobotB1']
                                #          ],
                                # 'processing machine': [['127.0.0.1', 7008], 'MachineA']
                                # }
                                send_msg(coord_addr, self.optimized_plan[(tuple(msg['start']), msg['task'])])
        Thread(target=start_pubsub_listener).start()
        Thread(target=start_socket_listener).start()


if __name__ == "__main__":
    args = sys.argv[1:]
    ip, port = args[0], int(args[1])
    Scheduler(ip, port).start()
