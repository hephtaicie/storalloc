""" Storalloc
    Hadware resources abstraction
"""

from dataclasses import dataclass, field

import yaml

from storalloc.utils.logging import get_storalloc_logger
from storalloc.request import StorageRequest

# pylint: disable=logging-fstring-interpolation


@dataclass
class DiskStatus:
    """Disk status class

    This object is used temporarily to store the status (bw, occupancy, ...)  of a disk
    while a scheduling algorithm is running
    """

    capacity: int = 0
    bandwidth: float = 0.0


@dataclass
class Disk:  # pylint: disable=too-many-instance-attributes
    """Disk class Define a disk from a node, including allocated space by jobs

    //!\\ Beware : when serialising, DiskStatus is lost.
    """

    uid: int
    vendor: str
    model: str
    serial: str
    capacity: int
    write_bandwidth: float
    read_bandwidth: float
    block_device: str
    allocations: "typing.Any" = field(default_factory=list)
    disk_status: DiskStatus = field(default_factory=DiskStatus, init=False, repr=False)

    def to_dict(self):
        """Export disk to dict format"""
        return {
            "uid": self.uid,
            "vendor": self.vendor,
            "model": self.model,
            "serial": self.serial,
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

    def __str__(self):
        """HR representation"""
        return (
            f"Disk {self.uid}//{self.serial} - [{self.vendor}/{self.model}] "
            + f"({self.capacity}:{self.read_bandwidth}:{self.write_bandwidth}) "
            + f"at {self.block_device}"
        )


@dataclass
class NodeStatus:
    """Node status class

    This object is used temporarily to store the status (bw, occupancy, ...)  of a node
    while a scheduling algorithm is running
    """

    bandwidth: float = 0.0
    #    disk_status: list[DiskStatus] = field(default_factory=list, init=False, repr=False)


@dataclass
class Node:
    """Node class
    Describe a storage node as a remote node with a set of disks

    //!\\ Beware : when serialising, NodeStatus is lost.
    """

    uid: int
    hostname: str = ""
    ipv4: str = ""
    bandwidth: float = 0.0
    node_status: NodeStatus = field(default_factory=NodeStatus, init=False, repr=False)
    disks: list[Disk] = field(default_factory=list, init=False, repr=False)

    def to_dict(self):
        """Dictionnary representation for serialisation"""
        return {
            "uid": self.uid,
            "hostname": self.hostname,
            "ipv4": self.ipv4,
            "bandwidth": self.bandwidth,
            "disks": [disk.to_dict() for disk in self.disks],
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create Node object from dict, for deserialisation"""
        node = cls(data["uid"], data["hostname"], data["ipv4"], data["bandwidth"])
        node.disks = [Disk.from_dict(disk) for disk in data["disks"]]
        return node


class ResourceCatalog:
    """ResourceCatalog class

    List of nodes and disks on nodes available for storage allocations.
    This list can evolve accoding to subscribed storage servers
    """

    def __init__(self, server_uid: str = None, yaml_file: str = None):
        """Init ResourceCatalog. Internal node storage is a dictionnary whose keys are server_id
        and values are list of nodes registered by this server.
        """

        self.log = get_storalloc_logger()
        self.storage_resources = {}
        if server_uid and yaml_file:
            self.nodes_from_yaml(server_uid, yaml_file)

    def nodes_from_yaml(self, server_uid, yaml_file):
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

                if not self.storage_resources.get(server_uid):
                    self.storage_resources[server_uid] = []

                self.storage_resources[server_uid].append(new_node)

        self.log.info(f"storage_resources catalog now contains {len(self.storage_resources)} nodes")

    def pretty_print(self):
        """Pretty print list of currently registered resources"""

        for server_id, nodes in self.storage_resources.items():
            for node in nodes:
                print(f"# Node {node.hostname} at index/uid {node.uid} from server {server_id}")
                print(f"  - IPv4 {node.ipv4}")
                print(f"  - Has {len(node.disks)} disks")
                for disk in node.disks:
                    print(disk)

    def add_allocation(self, server_id: str, node_id: int, disk_id: int, request: StorageRequest):
        """Add allocation in a given disk of a given node, for a specific request"""
        self.storage_resources[server_id][node_id].disks[disk_id].allocations.append(request)
        # After each insertion, ensure that allocations for disk are ordered such that
        # the end_time comes last
        self.storage_resources[server_id][node_id].disks[disk_id].allocations.sort(
            key=lambda x: -x.end_time.timestamp()
        )

    def get_node(self, server_id: str, node_id: int):
        """Get a specific node from list of resources"""
        return self.storage_resources[server_id][node_id]

    def node_count(self, server_id: str = None):
        """Return node count from resource list"""
        if server_id:
            return len(self.storage_resources.get(server_id))

        total_nodes = 0
        for nodes in self.storage_resources.values():
            total_nodes += len(nodes)
        return total_nodes

    def disk_count(self, server_id: str, node_id: int):
        """Return disk count for a specific node"""
        return len(self.storage_resources[server_id][node_id].disks)

    def disk_capacity(self, server_id: str, node_id: int, disk_id: int):
        """Return disk capacity for a specific disk in a specific node"""
        return self.storage_resources[server_id][node_id].disks[disk_id].capacity

    def is_empty(self):
        """Check whether any storage resources are populated or not"""
        if not self.storage_resources:
            return True
        return False

    def append_resources(self, server_id: str, resources: list[Node]):
        """Append the given resources received from a server to the catalog."""

        if self.storage_resources.get(server_id):
            self.storage_resources[server_id].extend(resources)
        else:
            self.storage_resources[server_id] = resources

    def list_resources(self):
        """Generator exposing every disks of every node registered in every server"""

        for server_id, nodes in self.storage_resources.items():
            for node in nodes:
                for disk in node.disks:
                    yield (server_id, node, disk)
