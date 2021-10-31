from mininet.topo import Topo
from utils import IP, NETMASK, MAC


class TestTopo(Topo):

    def build(self):

        switch = self.addSwitch('s1')

        cc = self.addHost(
            'cc',
            ip=IP['central controller'] + NETMASK,
            mac=MAC['central controller'])
        self.addLink(cc, switch)

        nNode = 10

        for i in range(nNode):
            name = 'Node' + str(i)
            node = self.addHost(
                name,
                ip=IP[name] + NETMASK,
                mac=MAC[name])
            self.addLink(node, switch)




