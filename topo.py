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

        ra1 = self.addHost(
            'ra1',
            ip=IP['ra1'] + NETMASK,
            mac=MAC['ra1'])
        self.addLink(ra1, switch)

