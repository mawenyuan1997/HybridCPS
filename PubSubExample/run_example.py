from mininet.net import Mininet
from mininet.cli import CLI
from topo import TestTopo
import utils
import sys
import time

class HybridCPS(object):

    def __init__(self, net):
        self.net = net
        net.start()
        # net.pingAll()

        pubsub = self.net.get('pubsub')
        pubsub.cmd('redis-server redis-stable/redis.conf &')
        pubsub.cmd('python3 HybridCPS/coordinator.py {} {} &'.format(utils.IP['pubsub'], utils.PORT['coordinator']))

    def stop(self):
        self.net.stop()

    def test_transition(self):
        RA1 = self.net.get('RA1')
        for i in range(10):
            RA1.cmd('python3 HybridCPS/ResourceAgent.py RA' + str(i) + ' &')

    def test_distributed(self):
        N1, N2, N3, N4 = self.net.get('Node1', 'Node2', 'Node3', 'Node4')

        N1.cmd('python3 HybridCPS/ResourceAgent.py {} {} {} 2 0 10 &'.format('RA1', utils.IP['Node1'], utils.PORT['RA1']))
        N2.cmd('python3 HybridCPS/ResourceAgent.py {} {} {} 1 20 10 &'.format('RA2', utils.IP['Node2'], utils.PORT['RA2']))
        N3.cmd('python3 HybridCPS/ResourceAgent.py {} {} {} 1 10 0 &'.format('RA3', utils.IP['Node3'], utils.PORT['RA3']))
        N4.cmd('python3 HybridCPS/ResourceAgent.py {} {} {} 1 20 0 &'.format('RA4', utils.IP['Node4'], utils.PORT['RA4']))
        time.sleep(1)
        N1.cmd('python3 HybridCPS/ProductAgent.py PA1 {} {} &'.format(utils.IP['Node1'], utils.PORT['PA1']))
        CLI(net)


if __name__ == "__main__":
    topo = TestTopo()
    net = Mininet(topo=topo)

    hybridcps = HybridCPS(net=net)
    hybridcps.test_distributed()
