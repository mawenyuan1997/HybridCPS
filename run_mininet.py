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

        pubsub = self.net.get('pubsub')
        pubsub.cmd('redis-server redis-stable/redis.conf &')
        pubsub.cmd('python3 HybridCPS/Coordinator.py {} {} &'.format(utils.IP['pubsub'], utils.PORT['coordinator']))

    def stop(self):
        self.net.stop()

    def test_distributed(self):
        Nodes = [None] * 4
        Nodes[0], Nodes[1], Nodes[2], Nodes[3] = self.net.get('Node1', 'Node2', 'Node3', 'Node4')
        n = 0
        config_dir = 'HybridCPS/SemiconductorMfg/RAconfig/'
        for config_file in os.listdir(config_dir):
            ra_name = config_file.split('.')[0]
            Nodes[n % 4].cmd('python3 HybridCPS/ResourceAgent.py {} {} {} {} &'.format(ra_name,
                                                                                       utils.IP[
                                                                                           'Node' + str(n % 4 + 1)],
                                                                                       utils.PORT['RA start'] + int(n/4),
                                                                                       config_dir + config_file))
            n += 1
        time.sleep(1)
        config_dir = 'HybridCPS/SemiconductorMfg/PAconfig/'
        Nodes[0].cmd('python3 HybridCPS/ProductAgent.py {} {} {} {} &'.format("PA1",
                                                                              utils.IP['Node1'],
                                                                              utils.PORT['PA start'],
                                                                              config_dir + "PA1.json"))
        Nodes[1].cmd('python3 HybridCPS/ProductAgent.py {} {} {} {} &'.format("PA2",
                                                                              utils.IP['Node2'],
                                                                              utils.PORT['PA start'],
                                                                              config_dir + "PA2.json"))
        CLI(net)


if __name__ == "__main__":
    topo = TestTopo()
    net = Mininet(topo=topo)

    hybridcps = SemiconductorMfg(net=net)
    hybridcps.test_distributed()