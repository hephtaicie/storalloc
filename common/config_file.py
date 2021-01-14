#!/usr/bin/env python3

import os
import sys
import yaml
import io
from pathlib import Path

class ConfigFile (object):

    def __init__ (self, path):
        super().__init__()
        
        self._content = None
        self._path = path

        if os.path.exists (self._path) and os.access (self._path, os.R_OK):
            self._parse_content ()
        else:
            print ('Error: the StorAlloc configuration file does not exist or is not readable ('+self._path+')')
            sys.exit(1)
            
            
    def _parse_content (self):
        stream = open (self._path, 'r')
        self._content = yaml.safe_load (stream)
        stream.close ()

        return self._content

    def get (self):
        return self._content
    
    
    def get_orch_hostname (self):
        return self._content['orchestrator']['hostname']


    def get_orch_ipv4 (self):
        return self._content['orchestrator']['ipv4']


    def get_orch_port (self):
        return int(self._content['orchestrator']['port'])

    
    def get_orch_strategy (self):
        return self._content['orchestrator']['sched_strategy']


    def get_orch_client_bind_ipv4 (self):
        return self._content['orchestrator']['client_bind']['ipv4']
            

    def get_orch_client_bind_port (self):
        return self._content['orchestrator']['client_bind']['port']
    

    def get_orch_server_bind_ipv4 (self):
        return self._content['orchestrator']['server_bind']['ipv4']
    

    def get_orch_server_bind_port (self):
        return self._content['orchestrator']['server_bind']['port']
