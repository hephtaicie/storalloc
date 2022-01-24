"""Round Robin Scheudling algorithm"""


from storalloc.strategies.base import StrategyInterface


class RoundRobin(StrategyInterface):
    """Round Robin scheduler"""

    def __init__(self):
        super().__init__()
        self.server_idx = 0
        self.node_idx = -1
        self.disk_indices = {}

    def compute(self, resource_catalog, request=None):
        """Compute chosen server,node,disk tuple based on a round robin scheduling"""

        # Select server from list
        server_list = list(resource_catalog.storage_resources.keys())
        s_idx = self.server_idx % len(server_list)
        server = server_list[s_idx]
        self.server_idx += 1

        # Increment node index
        if s_idx == 0:
            self.node_idx += 1

        # Select node:
        n_idx = self.node_idx % len(resource_catalog.storage_resources[server])
        node = resource_catalog.storage_resources[server][n_idx]
        node_key = f"{server}:{n_idx}"
        if node_key in self.disk_indices:
            d_idx = self.disk_indices[node_key] % len(node.disks)
            self.disk_indices[node_key] += 1
        else:
            self.disk_indices[node_key] = 1
            d_idx = 0

        return (server, n_idx, d_idx)
