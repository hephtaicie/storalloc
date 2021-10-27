""" Storalloc
    Hadware resources abstraction
"""

import yaml

from storalloc.logging import get_storalloc_logger

# TODO: turn resource classes into namedtuple definitions ?


class Node:
    """Node class

    Describe a storage node as a remote node with a set of disks
    """

    def __init__(self, idx, node):
        """Init a storage node from dict of parameters"""

        self.idx = idx
        self.identity = ""
        self.hostname = node["hostname"]
        self.ipv4 = node["ipv4"]
        self.bw = node["network_bw"]
        self.disks = []


class Disk:
    """Disk class

    Define a disk with a set of characteristics, including allocated space by jobs
    """

    def __init__(self, idx, disk):
        """Disk representation"""

        self.idx = idx
        self.vendor = disk["vendor"]
        self.model = disk["model"]
        self.serial = disk["serial"]
        self.capacity = disk["capacity"]
        self.w_bw = disk["w_bw"]
        self.r_bw = disk["r_bw"]
        self.blk_dev = disk["blk_dev"]
        self.allocs = []


class DiskStatus:
    """Disk status class

    This object is used temporarily to store the status (bw, occupancy, ...)  of a disk
    while a scheduling algorithm is running
    """

    def __init__(self, idx, capacity):
        """Disk status"""
        super().__init__()
        self.idx = idx
        self.bw = 0.0
        self.capacity = capacity


class NodeStatus:
    """Node status class

    This object is used temporarily to store the status (bw, occupancy, ...)  of a node
    while a scheduling algorithm is running
    """

    def __init__(self, idx):
        self.idx = idx
        self.bw = 0.0
        self.disk_status = []


class ResourceCatalog:
    """ResourceCatalog class

    List of nodes and disks on nodes available for storage allocations.
    This list can evolve accoding to subscribed storage servers
    """

    def __init__(self, yaml_file: str = None):
        """Init ResourceCatalog (with an empty list)"""

        self.log = get_storalloc_logger()
        self.storage_resources = []
        if yaml_file:
            self.populate_from_yaml(yaml_file)

    def populate_from_yaml(self, yaml_file):
        """Translate a system configuration file (YAML) into a set of storage resources."""

        self.log.info("Populating resource catalog from yaml file...")

        # TODO: file exists?
        with open(yaml_file, "r", encoding="utf-8") as yaml_stream:
            content = yaml.safe_load(yaml_stream)

            for idx_n, node in enumerate(content["hosts"]):
                new_node = Node(idx_n, node)
                for idx_d, disk in enumerate(node["disks"]):
                    new_disk = Disk(idx_d, disk)
                    new_node.disks.append(new_disk)
                self.storage_resources.append(new_node)

        self.log.info(f"storage_resources catalog now contains {len(self.storage_resources)} nodes")

    def add_allocation(self, node, disk, job):
        """Add allocation in a given disk of a given node, for a specific job"""
        self.storage_resources[node].disks[disk].allocs.append(job)

    def get_node(self, node):
        """Get a specific node from list of resources"""
        return self.storage_resources[node]

    def node_count(self):
        """Return node count from resource list"""
        return len(self.storage_resources)

    def disk_count(self, node):
        """Return disk count for a specific node"""
        return len(self.storage_resources[node].disks)

    def disk_capacity(self, node, disk):
        """Return disk capacity for a specific disk in a specific node"""
        return self.storage_resources[node].disks[disk].capacity

    def identity_of_node(self, node):
        """Get node ID"""
        self.log.debug(f"Querying identity of node : {node}")
        return self.storage_resources[node].identity

    def is_empty(self):
        """Check whether any storage resources are populated or not"""
        if not self.storage_resources:
            return True
        return False

    def append_resources(self, src_identity, resources):
        """Append the given resources received from a server to the catalog."""

        for node in resources:
            node.identity = src_identity
            self.storage_resources.append(node)

    def print_status(self, target_node_id, target_disk_id):
        """ASCII-based output of the given scheduling."""
        # TODO: Has to be deleted entirely at some point

        # Concatenate lists of requests per disk to determine ealiest start time and latest end time
        job_list = list()
        for n in range(0, len(self.storage_resources)):
            for d in range(0, len(self.storage_resources[n].disks)):
                job_list.extend(self.storage_resources[n].disks[d].allocs)

        if job_list:
            earliest_request = min([x.start_time for x in job_list])
            latest_request = max([x.end_time for x in job_list])
            steps = int((latest_request - earliest_request).seconds / 300)  # granularity: 5 minutes
        else:
            earliest_request = 0
            latest_request = 0
            steps = 0

        # Print the current status of the scheduling on nodes and disks
        for n in range(0, len(self.storage_resources)):
            print("┌───┬", end="")
            for s in range(0, steps):
                print("─", end="")
            print()
            for d in range(0, len(self.storage_resources[n].disks)):
                if not self.storage_resources[n].disks[d].allocs:
                    print("│" + str(d).rjust(3) + "│")
                else:
                    for i, j in enumerate(self.storage_resources[n].disks[d].allocs):
                        if i == 0:
                            print("│" + str(d).rjust(3) + "│", end="")
                        else:
                            print("│   │", end="")
                        offset = int((j.start_time - earliest_request).seconds / 300)
                        for o in range(0, offset):
                            print(" ", end="")
                        req_time = int((j.end_time - j.start_time()).seconds / 300)
                        req_time = 1 if req_time == 0 else req_time
                        for j in range(0, req_time):
                            if (
                                target_node_id == n
                                and target_disk_id == d
                                and i == len(self.storage_resources[n].disks[d].allocs) - 1
                            ):
                                print("□", end="")
                            else:
                                print("■", end="")
                        print()
                if d < len(self.storage_resources[n].disks) - 1:
                    print("├---┼", end="")
                    for s in range(0, steps):
                        print("-", end="")
                    print()
            print("└───┴", end="")
            for s in range(0, steps):
                print("─", end="")
            print()
