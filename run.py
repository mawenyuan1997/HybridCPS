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

    def test_distributed(self):
        PA1, RA1, RA2, RA3, RA4 = self.net.get('PA1', 'RA1', 'RA2', 'RA3', 'RA4')
        PA1.cmd('fixpython3 HybridCPS/ProductAgent.py PA1 &')
        RA1.cmd('python3 HybridCPS/ResourceAgent.py RA1 2 &')
        RA2.cmd('python3 HybridCPS/ResourceAgent.py RA2 1 &')
        RA3.cmd('python3 HybridCPS/ResourceAgent.py RA3 1 &')
        RA4.cmd('python3 HybridCPS/ResourceAgent.py RA4 1 &')


if __name__ == "__main__":
    topo = TestTopo()
    net = Mininet(topo=topo)

    hybridcps = HybridCPS(net=net)
    hybridcps.test_distributed()
