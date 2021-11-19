""" Base Scheduling strategy
"""

from abc import ABC, abstractmethod


class StrategyInterface(ABC):
    """Simple interface for scheduling strategies"""

    @abstractmethod
    def compute(self, resource_catalog, request):
        """Find an optimal allocation for a given request based on the current state of a
        storage resource catalog"""
