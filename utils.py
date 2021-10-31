import socket
import json

IP = {
    'Node0': '192.168.2.1',
    'Node1': '192.168.1.1',
    'Node2': '192.168.1.2',
    'Node3': '192.168.1.3',
    'Node4': '192.168.1.4',
    'Node5': '192.168.1.5',
    'Node6': '192.168.1.6',
    'Node7': '192.168.1.7',
    'Node8': '192.168.1.8',
    'Node9': '192.168.1.9',
    'Node10': '192.168.1.10',
    'Node11': '192.168.1.11',
    'central controller': '127.0.0.1',
    'coordinator': '127.0.0.1',
    'monitor': '127.0.0.1',
    'scheduler': '127.0.0.1'
}

PORT = {
    'pubsub': 6379,
    'coordinator': 4000,
    'monitor': 4001,
    'scheduler': 9000,
    'start': 8000,
    'RA start': 7000
}

NETMASK = '/24'

MAC = {
    'Node0': '00:1D:9C:C8:BD:F0',
    'Node1': '00:1D:9C:C7:B0:70',
    'Node2': '00:1D:9C:C8:BC:46',
    'Node3': '00:1D:9C:C8:BD:F2',
    'Node4': '00:1D:9C:C8:BD:F3',
    'Node5': '00:1D:9C:C8:BD:F5',
    'Node6': '00:1D:9C:C8:BD:F6',
    'Node7': '00:1D:9C:C8:BD:F7',
    'Node8': '00:1D:9C:C8:BD:F8',
    'Node9': '00:1D:9C:C8:BD:F9',
    'Node10': '00:2D:9C:C8:BD:F3',
    'Node11': '00:3D:9C:C8:BD:F3',
    'central controller': 'AA:AA:AA:AA:AA:AA',
}

BID_CONFIRM_TIMEOUT = 5
COMMAND_ORDER_TIMEOUT = 100000
STD_ERR = 1
INF = 1000000


def distance(a, b):
    return abs(a[0] - b[0]) + abs(a[1] - b[1])


def send_msg(addr, msg):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        ip, port = addr
        s.connect((ip, port))
        s.send(json.dumps(msg).encode())