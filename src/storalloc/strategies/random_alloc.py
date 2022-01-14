""" Storalloc
    Random allocation strategy
"""

import random

from storalloc.strategies.base import StrategyInterface


class RandomAlloc(StrategyInterface):
    """Random scheduler"""

    def compute(self, resource_catalog, request):
        """Make random allocation choice"""

        # resource_catalog.pretty_print()

        if resource_catalog.node_count() == 0:
            return ("", -1, -1)

        random.seed()
        server = random.choice(list(resource_catalog.storage_resources.keys()))
        target_node = random.randint(0, resource_catalog.node_count(server) - 1)
        target_disk = random.randint(0, resource_catalog.disk_count(server, target_node) - 1)

        return (server, target_node, target_disk)
