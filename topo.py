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
        for i in range(1, nRA + 1):
            name = 'RA' + str(i)
            RAi = self.addHost(
                name,
                ip=IP[name] + NETMASK,
                mac=MAC[name])
            self.addLink(RAi, switch)
            RAs.append(RAi)

        PAs = []
        for i in range(1, nPA + 1):
            name = 'PA' + str(i)
            PAi = self.addHost(
                name,
                ip=IP[name] + NETMASK,
                mac=MAC[name])
            self.addLink(PAi, switch)
            PAs.append(PAi)




