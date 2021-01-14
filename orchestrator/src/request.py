#!/usr/bin/env python3
import datetime as dt
import logging

class Request (object):

    def __init__ (self, idx, src_id, capacity, duration):
        super().__init__()
        self._idx        = idx
        self._src_id     = src_id
        self._capacity   = capacity
        self._duration   = duration
        self._status     = "pending"
        self._start_time = dt.datetime.now(dt.timezone.utc)
        self._end_time   = self._start_time + dt.timedelta(minutes=self._duration)


    def print_request (self):
        print (self.request_string ())

    def request_string (self):
        return ("["+str(self._idx)+", "+str(self._capacity)+" GB, "+
                str(self._duration)+" m, "+str(self._start_time)+", "+
                str(self._end_time)+"]")

    def get_idx (self):
        return self._idx

    def get_identity (self):
        return self._src_id

    def set_status (self, status):
        self._status = status

    def get_status (self):
        return self._status
        
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
