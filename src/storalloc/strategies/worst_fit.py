"""Worst fit scheduling algorithm"""

import random
from storalloc.strategies.base import StrategyInterface


class WorstFit(StrategyInterface):
    """Worst fit scheduler"""

    def compute(self, resource_catalog, request):
        """Compute chosen server,node,disk tuple based on a round robin scheduling"""

        disks_free_space = []

        for server, node, disk in resource_catalog.list_resources():
            free_space = disk.capacity
            for allocation in disk.allocations:
                if allocation.overlaps(request):
                    free_space -= allocation.capacity
            free_space = free_space * 100 / disk.capacity
            disks_free_space.append((server, node.uid, disk.uid, free_space))

        disks_free_space.sort(key=lambda x: -x[3])
        max_free_space = disks_free_space[0][3]
        candidates = [disks_free_space[0]]
        for tpl in disks_free_space:
            if tpl[3] < max_free_space:
                break
            candidates.append(tpl)

        return random.choice(candidates)[:3]
