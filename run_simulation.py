from mininet.net import Mininet
from mininet.cli import CLI
from topo import TestTopo
import utils
import os
import time


class SemiconductorMfg(object):

    def __init__(self, net):
        self.net = net
        net.start()
        # net.pingAll()
        self.nodes = [None] * 4
        self.nodes[0], self.nodes[1], self.nodes[2], self.nodes[3] = self.net.get('Node1', 'Node2', 'Node3', 'Node4')
        pubsub = self.net.get('pubsub')
        pubsub.cmd('redis-server redis-stable/redis.conf &')
        pubsub.cmd('python3 HybridCPS/Coordinator.py {} {} &'.format(utils.IP['pubsub'], utils.PORT['coordinator']))

    def stop(self):
        self.net.stop()

    def test_distributed(self):

        n = 0
        config_dir = 'HybridCPS/SemiconductorMfg/RAconfig/'
        for config_file in os.listdir(config_dir):
            ra_name = config_file.split('.')[0]
            self.nodes[n % 4].cmd('python3 HybridCPS/ResourceAgent.py {} {} {} {} &'.format(ra_name,
                                                                                       utils.IP[
                                                                                           'Node' + str(n % 4 + 1)],
                                                                                       utils.PORT['RA start'] + int(n/4),
                                                                                       config_dir + config_file))
            n += 1
        time.sleep(1)
        CLI(net)


if __name__ == "__main__":
    topo = TestTopo()
    net = Mininet(topo=topo)

    hybridcps = SemiconductorMfg(net=net)
    hybridcps.test_distributed()
