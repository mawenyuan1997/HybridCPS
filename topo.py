from mininet.topo import Topo
from utils import IP, NETMASK, MAC


class TestTopo(Topo):

    def build(self):

        switch = self.addSwitch('s1')

        pubsub = self.addHost(
            'pubsub',
            ip=IP['pubsub'] + NETMASK,
            mac=MAC['pubsub'])
        self.addLink(pubsub, switch)

        nNode = 4

        for i in range(1, nNode + 1):
            name = 'Node' + str(i)
            node = self.addHost(
                name,
                ip=IP[name] + NETMASK,
                mac=MAC[name])
            self.addLink(node, switch)




