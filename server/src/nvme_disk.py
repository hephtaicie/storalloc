#!/usr/bin/env python3

class NVMeDisk (object):

    def __init__ (self, idx, disk):
        super().__init__()

        self._idx      = idx

        self._vendor   = disk['vendor']
        self._model    = disk['model']
        self._serial   = disk['serial']
        self._capacity = disk['capacity']
        self._w_bw     = disk['w_bw']
        self._r_bw     = disk['r_bw']
        self._blk_dev  = disk['blk_dev']

        self.allocs    = list ()

    def get_idx (self):
        return self._idx

    def get_bw (self):
        return self._w_bw

    def get_capacity (self):
        return self._capacity

    def get_blk_dev (self):
        return self._blk_dev
