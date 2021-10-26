#!/usr/bin/env python3

import sys
from src.strategies.worst_case import WorstCase
from src.strategies.random_alloc import RandomAlloc


class SchedStrategy(object):
    def __init__(self):
        super().__init__()
        self._strategy_str = ""
        self._strategy = None
        self._target_hostname = ""
        self._target_ip = ""
        self._target_port = -1
        self._target_disk = None

    def set_strategy(self, strategy):
        self._strategy_str = strategy

        if self._strategy_str == "random_alloc":
            self._strategy = RandomAlloc()
        elif self._strategy_str == "worst_case":
            self._strategy = WorstCase()
        else:
            print(
                "Error: the scheduling strategy specified in the configuration file does not exist!"
            )
            sys.exit(1)

    def compute(self, resource_catalog, job):
        if resource_catalog.is_empty():
            return -1, -1

        return self._strategy.compute(resource_catalog, job)
