#!/usr/bin/env python3

import datetime as dt
import logging

class Request (object):

    def __init__ (self, request):
        super().__init__()

        capacity   = int(request.split(',')[0])
        duration   = int(request.split(',')[1])
        if request.split(',')[2] != "None":
            start_time = dt.datetime.strptime(request.split(',')[2], '%Y-%m-%d %H:%M:%S')
        else:
            start_time = None
                
        if capacity <= 0 or duration <= 0:
            raise ValueError
        
        self._capacity   = capacity
        self._duration   = duration
        self._start_time = start_time

        
    def print_request (self):
        print (self.to_string ())

        
    def to_string (self):
        return ("["+str(self._capacity)+" GB, "
                +str(dt.timedelta(seconds=self._duration))+", "
                +str(self._start_time)+"]")

    
    def capacity (self):
        return self._capacity

    
    def duration (self):
        return self._duration


    def start_time (self):
        return self._start_time
