IP = {
    'Node1': '192.168.1.1',
    'Node2': '192.168.1.2',
    'Node3': '192.168.1.3',
    'Node4': '192.168.1.4',
    'pubsub': '127.0.0.1',
    'coordinator': '127.0.0.1',
    'central controller': '127.0.0.1'
}

PORT = {
    'pubsub': 6379,
    'coordinator': 5000,
    'central controller': 9000,
    'PA start': 8000,
    'RA start': 7000
}

NETMASK = '/24'

MAC = {
    'Node1': '00:1D:9C:C7:B0:70',
    'Node2': '00:1D:9C:C8:BC:46',
    'Node3': '00:1D:9C:C8:BD:F2',
    'Node4': '00:1D:9C:C8:BD:F3',
    'pubsub': 'AA:AA:AA:AA:AA:AA',
}

BID_CONFIRM_TIMEOUT = 5
COMMAND_ORDER_TIMEOUT = 100000
STD_ERR = 1