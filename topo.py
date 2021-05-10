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

        nRA = 4
        nPA = 2

        RAs = []
        for i in range(nRA):
            RAi = self.addHost(
                'RA' + i,
                ip=IP['RA' + i] + NETMASK,
                mac=MAC['RA' + i])
            self.addLink(RAi, switch)
            RAs.append(RAi)

        PAs = []
        for i in range(nPA):
            PAi = self.addHost(
                'PA' + i,
                ip=IP['PA' + i] + NETMASK,
                mac=MAC['PA' + i])
            self.addLink(PAi, switch)
            PAs.append(PAi)




