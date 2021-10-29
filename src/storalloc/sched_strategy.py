""" Storalloc
    Scheduler stargegy
"""

import sys
from storalloc.strategies.worst_case import WorstCase
from storalloc.strategies.random_alloc import RandomAlloc
from storalloc.logging import get_storalloc_logger


class SchedStrategy:
    """Choice of scheduling algorithm"""

    def __init__(self):
        self.log = get_storalloc_logger()
        self._strategy_str = ""
        self._strategy = None
        self._target_hostname = ""
        self._target_ip = ""
        self._target_port = -1
        self._target_disk = None

    def set_strategy(self, strategy):
        """Set a strategy"""

        self._strategy_str = strategy

        if self._strategy_str == "random_alloc":
            self._strategy = RandomAlloc()
        elif self._strategy_str == "worst_case":
            self._strategy = WorstCase()
        else:
            self.log.error(
                f"The scheduling strategy {strategy} specified in configuration does not exist"
            )
            sys.exit(1)

    def compute(self, resource_catalog, job):
        """Actually call the chosen scheduling strategy"""

        if resource_catalog.is_empty():
            return (-1, -1)

        return self._strategy.compute(resource_catalog, job)
