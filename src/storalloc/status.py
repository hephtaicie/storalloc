""" Storalloc
    Default status file representation
"""

import os
import sys
from pathlib import Path
import yaml

from storalloc.logging import get_storalloc_logger


class StatusFile:
    """Status file abstraction"""

    def __init__(self, path):
        """Init status file (create or read existing one)"""

        self.log = get_storalloc_logger()
        self._path = path

        if os.path.exists(self._path):
            if os.access(self._path, os.R_OK) and os.access(self._path, os.W_OK):
                self.get_content()
            else:
                self.log.error("A status file exists but cannot be accessed {path}")
                sys.exit(1)
        else:
            try:
                Path(self._path).touch()
            except:
                self.log.error("Cannot create the status file at {path}")
                sys.exit(1)

    def get_content(self):
        """Read current content of status file"""
        with open(self._path, "r", encoding="utf-8") as yaml_stream:
            return yaml.safe_load(yaml_stream)

    def update_content(self, new_content):
        """Update status file"""
        with open(self._path, "w", encoding="utf8") as yaml_stream:
            yaml.dump(new_content, yaml_stream, default_flow_style=False, allow_unicode=True)
