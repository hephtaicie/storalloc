""" Tests for storalloc.resources
"""

import datetime as dt
import pytest

from storalloc import resources as rs
from storalloc import request as rq

# pylint: disable=no-value-for-parameter,invalid-name, redefined-outer-name


@pytest.fixture
def disk_list():
    """Fixture for a list of valid disks"""
    return [
        rs.Disk(
            uid=3,
            vendor="adata",
            model="sx8200",
            serial="ATAT64",
            capacity=120,
            write_bandwidth=3.0,
            read_bandwidth=3.5,
            block_device="/dev/nvme1p1",
        ),
        rs.Disk(
            uid=3,
            vendor="adata",
            model="sx8200",
            serial="HDSA66",
            capacity=120,
            write_bandwidth=3.0,
            read_bandwidth=3.5,
            block_device="/dev/nvme0p2",
        ),
        rs.Disk(
            uid=3,
            vendor="adata",
            model="sx8200",
            serial="UDS712",
            capacity=120,
            write_bandwidth=3.0,
            read_bandwidth=3.5,
            block_device="/dev/nvme0p3",
        ),
    ]


@pytest.fixture
def resource_catalog():
    """Return a resource catalog and server_id"""

    server_id = "S-15362"
    rc = rs.ResourceCatalog(server_id, "tests/dummy_system.yml")
    return (server_id, rc)


def test_disk():
    """Test for Disk dataclass"""

    with pytest.raises(TypeError):
        rs.Disk()

    d1 = rs.Disk(
        uid=3,
        vendor="adata",
        model="sx8200",
        serial="ATAT62",
        capacity=120,
        write_bandwidth=3.0,
        read_bandwidth=3.5,
        block_device="/dev/nvme0p1",
    )
    assert d1.allocations == []
    assert d1.disk_status == rs.DiskStatus()

    # export to dict
    d1_dict = d1.to_dict()
    assert d1_dict["uid"] == d1.uid
    assert d1_dict["vendor"] == d1.vendor
    assert d1_dict["model"] == d1.model
    assert d1_dict["serial"] == d1.serial
    assert d1_dict["capacity"] == d1.capacity
    assert d1_dict["write_bandwidth"] == d1.write_bandwidth
    assert d1_dict["read_bandwidth"] == d1.read_bandwidth
    assert d1_dict["block_device"] == d1.block_device
    assert d1_dict["allocations"] == d1.allocations
    assert "disk_status" not in d1_dict

    # Import from dict
    d1_b = rs.Disk.from_dict(d1_dict)
    assert d1_b == d1

    # STR
    assert "Disk 3//ATAT62 - [adata/sx8200]" in f"{d1}"


def test_disk_status():
    """Test DiskStatus dataclass"""

    ds1 = rs.DiskStatus()
    assert ds1.capacity == 0
    assert ds1.bandwidth == 0.0


def test_node(disk_list):
    """Test Node"""

    with pytest.raises(TypeError):
        rs.Node()

    n1 = rs.Node(uid=2)
    assert n1.uid == 2
    assert n1.hostname == ""
    assert n1.ipv4 == ""
    assert n1.bandwidth == 0.0
    assert n1.node_status == rs.NodeStatus()
    assert n1.disks == []

    n1.hostname = "node1.host"
    n1.ipv4 = "127.0.0.1"
    n1.bandwidth = 12.5
    n1.disks = disk_list
    n1_dict = n1.to_dict()
    assert n1_dict["uid"] == n1.uid
    assert n1_dict["hostname"] == n1.hostname
    assert n1_dict["ipv4"] == n1.ipv4
    assert n1_dict["bandwidth"] == n1.bandwidth
    assert n1_dict["disks"] == [disk.to_dict() for disk in n1.disks]
    assert "node_status" not in n1_dict

    assert rs.Node.from_dict(n1_dict) == n1


def test_resource_catalog_from_yaml():
    """Create a resource catalog and populate it with yaml file"""

    rc = rs.ResourceCatalog("S-15362", "tests/dummy_system.yml")
    assert "S-15362" in rc.storage_resources


def test_resource_catalog_node_access(resource_catalog):
    """Test accessing node(s) informations in ResourceCatalog"""

    server_id, rc = resource_catalog

    assert rc.get_node(server_id, 0).hostname == "test_system"
    assert rc.get_node(server_id, 0).ipv4 == "148.42.123.85"
    assert rc.get_node(server_id, 0).bandwidth == 12.5
    assert rc.node_count(server_id) == 1

    with pytest.raises(IndexError):
        rc.get_node(server_id, 1)

    with pytest.raises(KeyError):
        rc.get_node("myserver", 0)


def test_resource_catalog_disk_access(resource_catalog):
    """Test accessing disk(s) informations in ResourceCatalog"""

    server_id, rc = resource_catalog
    assert rc.disk_count(server_id, 0) == 2
    assert rc.disk_capacity(server_id, 0, 0) == 4000

    with pytest.raises(IndexError):
        rc.disk_count(server_id, 2)

    with pytest.raises(IndexError):
        rc.disk_capacity(server_id, 0, 4)

    with pytest.raises(KeyError):
        rc.disk_count("myserver", 0)

    with pytest.raises(KeyError):
        rc.disk_capacity("myserver", 0, 0)


def test_append_resources(disk_list):
    """Append resources to a ResourceCatalog, after creation"""

    node = rs.Node(uid=0, hostname="new_node")
    node.disks = disk_list

    rc = rs.ResourceCatalog()
    server_id = "new_server"
    rc.append_resources(server_id, [node])

    assert rc.get_node(server_id, 0).hostname == "new_node"
    assert rc.get_node(server_id, 0).disks == disk_list

    other_node = rs.Node(uid=47, hostname="other_node")
    rc.append_resources(server_id, [other_node])
    assert rc.get_node(server_id, 1).hostname == "other_node"
    assert rc.get_node(server_id, 1).disks == []
    assert rc.node_count(server_id) == 2

    assert rc.node_count() == 2


def test_empty_storage_resource(resource_catalog):
    """Test if a storage resource is empty or not"""

    _, rc = resource_catalog
    assert not rc.is_empty()

    rc2 = rs.ResourceCatalog()
    assert rc2.is_empty()


def test_add_allocation(resource_catalog):
    """Adding allocation on a ResourceCatalog node/disk"""

    server_id, rc = resource_catalog

    # Random storage request
    start_time = dt.datetime.now()
    req1 = rq.StorageRequest(capacity=20, duration=dt.timedelta(hours=3), start_time=start_time)
    req1.state = rq.ReqState.ALLOCATED
    rc.add_allocation(server_id, 0, 1, req1)
    assert rc.get_node(server_id, 0).disks[1].allocations == [req1]

    # Check allocation re-ordering (the sooner the allocation ends,
    # the further back at the end of the list it is):
    req2 = rq.StorageRequest(capacity=20, duration=dt.timedelta(hours=5), start_time=start_time)
    req2.state = rq.ReqState.ALLOCATED
    rc.add_allocation(server_id, 0, 1, req2)
    assert rc.get_node(server_id, 0).disks[1].allocations == [req2, req1]

    req3 = rq.StorageRequest(capacity=20, duration=dt.timedelta(hours=4), start_time=start_time)
    req3.state = rq.ReqState.ALLOCATED
    rc.add_allocation(server_id, 0, 1, req3)
    assert rc.get_node(server_id, 0).disks[1].allocations == [req2, req3, req1]


def test_pretty_print(resource_catalog, capsys):
    """Test ResourceCatalog pretty print function"""

    server_id, rc = resource_catalog
    rc.pretty_print()
    captured = capsys.readouterr()
    assert f"# Node test_system at index/uid 0 from server {server_id}" in captured.out
