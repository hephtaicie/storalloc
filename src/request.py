#!/usr/bin/env python3
import datetime as dt
import logging

class Request (object):

    def __init__ (self, request):
        super().__init__()

        capacity = int(request.split(',')[0])
        duration = int(request.split(',')[1])
                
        if capacity <= 0 or duration <= 0:
            raise ValueError
        
        self._capacity   = capacity
        self._duration   = duration

        
    def print_request (self):
        print (self.to_string ())

        
    def to_string (self):
        return ("["+str(self._capacity)+" GB, "+str(self._duration)+" m]")

    
    def capacity (self):
        return self._capacity

    
    def duration (self):
        return self._duration

