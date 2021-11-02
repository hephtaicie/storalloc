""" Storalloc
    Hadware resources abstraction
"""

from dataclasses import dataclass, field

import yaml

from storalloc.logging import get_storalloc_logger


@dataclass
class Disk:
    """Disk class Define a disk from a node, including allocated space by jobs"""

    uid: int
    vendor: str
    model: str
    serial: str
    capacity: int
    write_bandwidth: float
    read_bandwidth: float
    block_device: str
    allocations: "typing.Any" = field(default_factory=list)

    def to_dict(self):
        """Export disk to dict format"""
        return {
            "uid": self.uid,
            "vendor": self.vendor,
            "model": self.serial,
            "capacity": self.capacity,
            "write_bandwidth": self.write_bandwidth,
            "read_bandwidth": self.read_bandwidth,
            "block_device": self.block_device,
            "allocations": self.allocations,
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create disk from dictionnary (either for yaml import or deserialisation"""
        disk = cls(
            data["uid"],
            data["vendor"],
            data["model"],
            data["serial"],
            data["capacity"],
            data["write_bandwidth"],
            data["read_bandwidth"],
            data["block_device"],
        )
        disk.allocations = data["allocations"] if data.get("allocations") else []
        return disk


@dataclass
class Node:
    """Node class
    Describe a storage node as a remote node with a set of disks
    """

    uid: int
    hostname: str = ""
    ipv4: str = ""
    bandwidth: float = 0
    disks: list[Disk] = field(default_factory=list, init=False, repr=False)
    identity: str = field(default="", init=False)

    def to_dict(self):
        """Dictionnary representation for serialisation"""
        return {
            "uid": self.uid,
            "hostname": self.hostname,
            "ipv4": self.ipv4,
            "bandwidth": self.bandwidth,
            "identidy": self.identity,
            "disks": [disk.to_dict() for disk in self.disks],
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create Node object from dict, for deserialisation"""
        node = cls(data["uid"], data["hostname"], data["ipv4"], data["bandwidth"])
        node.identity = data["identity"] if data.get("identity") else ""
        node.disks = data["disks"]
        return node


@dataclass
class DiskStatus:
    """Disk status class

    This object is used temporarily to store the status (bw, occupancy, ...)  of a disk
    while a scheduling algorithm is running
    """

    uid: int
    capacity: int
    bandwidth: float = 0.0


@dataclass
class NodeStatus:
    """Node status class

    This object is used temporarily to store the status (bw, occupancy, ...)  of a node
    while a scheduling algorithm is running
    """

    uid: int
    bandwidth: float = 0.0
    disk_status: list[DiskStatus] = field(default_factory=list, init=False, repr=False)


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

        with open(yaml_file, "r", encoding="utf-8") as yaml_stream:
            content = yaml.safe_load(yaml_stream)

            for index, node in enumerate(content["hosts"]):

                new_node = Node(
                    uid=index,  # So far we use the index as node identifier, that may change
                    hostname=node["hostname"],
                    ipv4=node["ipv4"],
                    bandwidth=node["network_bw"],
                )

                for dindex, disk in enumerate(node["disks"]):
                    disk["uid"] = dindex
                    new_disk = Disk.from_dict(disk)
                    new_node.disks.append(new_disk)

                self.storage_resources.append(new_node)

        self.log.info(f"storage_resources catalog now contains {len(self.storage_resources)} nodes")

    def add_allocation(self, node_id: int, disk_id: int, job):
        """Add allocation in a given disk of a given node, for a specific job"""
        self.storage_resources[node_id].disks[disk_id].allocations.append(job)

    def get_node(self, node_id: int):
        """Get a specific node from list of resources"""
        return self.storage_resources[node_id]

    def node_count(self):
        """Return node count from resource list"""
        return len(self.storage_resources)

    def disk_count(self, node_id: int):
        """Return disk count for a specific node"""
        return len(self.storage_resources[node_id].disks)

    def disk_capacity(self, node_id: int, disk_id: int):
        """Return disk capacity for a specific disk in a specific node"""
        return self.storage_resources[node_id].disks[disk_id].capacity

    def identity_of_node(self, node_id: int):
        """Get node ID"""
        self.log.debug(f"Querying identity of node : {node_id}")
        return self.storage_resources[node_id].identity

    def is_empty(self):
        """Check whether any storage resources are populated or not"""
        if not self.storage_resources:
            return True
        return False

    def append_resources(self, src_identity: int, resources: list[Node]):
        """Append the given resources received from a server to the catalog."""

        for node in resources:
            node.identity = src_identity
            self.storage_resources.append(node)

    def print_status(self, target_node_id, target_disk_id):
        """ASCII-based output of the given scheduling."""
        # TODO: Has to be deleted entirely at some point

        # Concatenate lists of requests per disk to determine ealiest start time and latest end time
        job_list = []
        for node in self.storage_resources:
            for disk in node.disks:
                job_list.extend(disk.allocations)

        if job_list:
            earliest_request = min([x.start_time for x in job_list])
            latest_request = max([x.end_time for x in job_list])
            steps = int((latest_request - earliest_request).seconds / 300)  # granularity: 5 minutes
        else:
            earliest_request = 0
            latest_request = 0
            steps = 0

        # Print the current status of the scheduling on nodes and disks
        for node in self.storage_resources:
            print("┌───┬", end="")
            for _ in range(0, steps):
                print("─", end="")
            print()
            for disk in node.disks:
                if not disk.allocations:
                    print(f"│{disk:>3}│")
                else:
                    for idx, alloc in enumerate(disk.allocations):
                        if idx == 0:
                            print(f"│{disk:>3}│", end="")
                        else:
                            print("│   │", end="")

                        offset = int((alloc.start_time - earliest_request).seconds / 300)

                        for _ in range(0, offset):
                            print(" ", end="")

                        req_time = int((alloc.end_time - alloc.start_time).seconds / 300)
                        req_time = 1 if req_time == 0 else req_time

                        for _ in range(0, req_time):
                            if (
                                target_node_id == node.uid
                                and target_disk_id == disk.uid
                                and idx == len(disk.allocation) - 1
                            ):
                                print("□", end="")
                            else:
                                print("■", end="")
                        print()

                if disk.uid < len(node.disks) - 1:
                    print("├---┼", end="")
                    for _ in range(0, steps):
                        print("-", end="")
                    print()
            print("└───┴", end="")
            for _ in range(0, steps):
                print("─", end="")
            print()
