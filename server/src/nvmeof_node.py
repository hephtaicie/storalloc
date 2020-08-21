#!/usr/bin/env python3 

class NVMeoFNode (object):

    def __init__ (self, idx, node):
        super().__init__()

        self._idx      = idx
        self._hostname = node['hostname']
        self._ipv4     = node['ipv4']
        self._bw       = node['network_bw']

        self.disks = list ()

    def get_idx (self):
        return self._idx

    def get_ipv4 (self):
        return self._ipv4
    
    def get_bw (self):
        return self._bw
