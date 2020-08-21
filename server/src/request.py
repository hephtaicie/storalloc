#!/usr/bin/env python3
import datetime as dt
import logging
from src.allocation import Allocation

class Request (object):

    def __init__ (self, idx, capacity, duration):
        super().__init__()
        self._idx        = idx
        self._capacity   = capacity
        self._duration   = duration
        self._start_time = dt.datetime.now(dt.timezone.utc)
        self._end_time   = self._start_time + dt.timedelta(minutes=self._duration)
        self._allocation = Allocation ()


    def print_request (self):
        print (self.request_string ())

    def request_string (self):
        return ("["+str(self._idx)+", "+str(self._capacity)+" GB, "+
                str(self._duration)+" m, "+str(self._start_time)+", "+
                str(self._end_time)+"]")

    def get_idx (self):
        return self._idx
    
    def get_capacity (self):
        return self._capacity

    def get_start_time (self):
        return self._start_time

    def get_end_time (self):
        return self._end_time

    def get_duration (self):
        return self._duration

    def get_timediff (self):
        return self._end_time - self._start_time

    def allocate_resources (self, node_ipv4, device):
        nqn, port = self._allocation.create_disk_allocation(node_ipv4, device, self._capacity)
        return nqn, port
