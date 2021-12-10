""" Base Scheduling strategy
"""

from abc import ABC, abstractmethod

import logging


class StrategyInterface(ABC):
    """Simple interface for scheduling strategies"""

    def __init__(self):
        """Define a custom local logger, just in case none is provided later on"""
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG)

    def set_logger(self, logger):
        """Set local logger to an externally provided one
        (with remote logging abilities for instance)
        """
        self.log = logger

    @abstractmethod
    def compute(self, resource_catalog, request):
        """Find an optimal allocation for a given request based on the current state of a
        storage resource catalog"""
