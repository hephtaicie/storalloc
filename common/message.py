#!/usr/bin/env python3

import os
import sys
import pickle


class Message (object):

    def __init__ (self, msg_type, msg_content):
        super().__init__()

        self._type    = msg_type
        self._content = msg_content

        
    @classmethod
    def from_packed_message (cls, packed_data):
        return pickle.loads (packed_data)
        

    def pack (self):
        return pickle.dumps (self)
    
    
    def get_type (self):
        return self._type


    def get_content (self):
        return self._content
