from mininet.net import Mininet
from mininet.cli import CLI

from topo import TestTopo

import sys


class HybridCPS(object):

    def __init__(self, net):
        self.net = net
        net.start()
        # net.pingAll()

        nRA = 4
        nPA = 2

        pubsub = self.net.get('pubsub')
        pubsub.cmd('redis-server redis-stable/redis.conf &')

    def stop(self):
        self.net.stop()

    def test_transition(self):
        RA1 = self.net.get('RA1')
        for i in range(10):
            RA1.cmd('python3 HybridCPS/ResourceAgent.py RA' + str(i) + ' &')
        CLI(net)



if __name__ == "__main__":
    topo = TestTopo()
    net = Mininet(topo=topo)

    hybridcps = HybridCPS(net=net)
    hybridcps.test_transition()
