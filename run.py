from mininet.net import Mininet
from mininet.cli import CLI

from topo import TestTopo

import sys

class HybridCPS(object):

    def __init__(self, net):
        self.net = net
        net.start()
        net.pingAll()

        pubsub = self.net.get('pubsub')
        pubsub.cmd('redis-server redis-stable/redis.conf &')

        # net.stop()


if __name__ == "__main__":

    topo = TestTopo()
    net = Mininet(topo=topo)

    hybridcps = HybridCPS(net=net)