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
        pubsub.cmd('python3 HybridCPS/Coordinator.py {} {} &'.format('192.168.1.100', 7000))

    def stop(self):
        self.net.stop()

    def test_transition(self):
        RA1 = self.net.get('RA1')
        for i in range(10):
            RA1.cmd('python3 HybridCPS/ResourceAgent.py RA' + str(i) + ' &')

    def test_distributed(self):
        PA1, RA1, RA2, RA3, RA4 = self.net.get('PA1', 'RA1', 'RA2', 'RA3', 'RA4')

        PA1.cmd('python3 HybridCPS/ProductAgent.py PA1 &')
        RA1.cmd('python3 HybridCPS/ResourceAgent.py RA1 2 0 10 &')
        RA2.cmd('python3 HybridCPS/ResourceAgent.py RA2 1 20 10 &')
        RA3.cmd('python3 HybridCPS/ResourceAgent.py RA3 1 10 0 &')
        RA4.cmd('python3 HybridCPS/ResourceAgent.py RA4 1 20 0 &')
        CLI(net)


if __name__ == "__main__":
    topo = TestTopo()
    net = Mininet(topo=topo)

    hybridcps = HybridCPS(net=net)
    hybridcps.test_distributed()
