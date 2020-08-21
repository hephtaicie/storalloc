#!/usr/bin/env python3

import os
import sys
import yaml
import io
from pathlib import Path

class StatusFile (object):

    def __init__ (self, path):
        super().__init__()
        
        self._content = None
        self._path = path

        if os.path.exists (self._path):
            if os.access (self._path, os.R_OK) and os.access (self._path, os.W_OK):
                self.get_content ()
            else:
                print ('Error: a status file exists but cannot be accessed ('+self._path+')')
                sys.exit(1)
        else:
            try:
                Path(self._path).touch()
            except:
                print ('Error: cannot create the status file ('+self._path+')')
                sys.exit(1)


    def get_content (self):
        stream = open (self._path, 'r')
        self._content = yaml.safe_load (stream)
        stream.close ()

        return self._content

        
    def update_content (self, new_content):
        with open (self._path, 'w',  encoding='utf8') as stream:
            yaml.dump (new_content, stream, default_flow_style=False, allow_unicode=True)
        stream.close ()
        self._content = new_content
