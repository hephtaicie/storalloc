""" Storalloc
    ConfigFile parser
"""

import os
import sys
import yaml


class ConfigFile:
    """Representation of a config file, parsed from YAML file"""

    def __init__(self, path):
        """Init config from file path"""

        if os.path.exists(path) and os.access(path, os.R_OK):
            with open(path, "r", encoding="utf-8") as yaml_stream:
                self.content = yaml.safe_load(yaml_stream)
        else:
            print(f"Error: provided path for configuration seems to be invalid :{path}")
            sys.exit(1)

    # TODO : some of those config names are rather misleading (what do they point to exactly ?)
    def get_orch_hostname(self):
        """Get orchestrator hostname"""
        return self.content["orchestrator"]["hostname"]

    def get_orch_ipv4(self):
        """Get orchestrator IPv4 address"""
        return self.content["orchestrator"]["ipv4"]

    def get_orch_port(self):
        """Get orchestrator linstening port"""
        return int(self.content["orchestrator"]["port"])

    def get_orch_strategy(self):
        """Get orchestrator strategy"""
        return self.content["orchestrator"]["sched_strategy"]

    def get_orch_client_bind_ipv4(self):
        """Get orchestrator client hostname"""
        return self.content["orchestrator"]["client_bind"]["ipv4"]

    def get_orch_client_bind_port(self):
        """Get orchestrator client binding port"""
        return self.content["orchestrator"]["client_bind"]["port"]

    def get_orch_server_bind_ipv4(self):
        """Get orchestrator server hostname"""
        return self.content["orchestrator"]["server_bind"]["ipv4"]

    def get_orch_server_bind_port(self):
        """Get orchestrator server binding port"""
        return self.content["orchestrator"]["server_bind"]["port"]
