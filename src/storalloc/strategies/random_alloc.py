""" Storalloc
    Random allocation strategy
"""

import random


class RandomAlloc:
    """Random scheduler"""

    def compute(self, resource_catalog, job):
        """Make random allocation choice"""

        # TODO change random values so that they are actually bounded
        #      by # of nodes in catalog / # number of disk in chosen node
        # TODO : Why do we need 'job' here ?

        random.seed()
        target_node = random.randint(0, resource_catalog.node_count() - 1)
        target_disk = random.randint(0, resource_catalog.disk_count(target_node) - 1)

        return (target_node, target_disk)
