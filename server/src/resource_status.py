#!/usr/bin/env python3

class DiskStatus (object):

    def __init__ (self, idx, capacity):
        super().__init__()
        self._idx     = idx
        self.bw       = 0.0
        self.capacity = capacity

    def get_idx (self):
        return self._idx


class NodeStatus (object):

    def __init__ (self, idx):
        super().__init__()
        self._idx        = idx
        self.bw          = 0.0
        self.disk_status = list ()

    def get_idx (self):
        return self._idx
