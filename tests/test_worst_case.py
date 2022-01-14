""" Testing "worst_case" scheduling strategy
"""

import datetime as dt
import pytest


from storalloc import resources
from storalloc import request
from storalloc.strategies import worst_case as wcase


@pytest.fixture
def resource_catalog():
    """Return a resource catalog and server_id"""

    server_id = "S-15362"
    return (server_id, resources.ResourceCatalog(server_id, "tests/dummy_system.yml"))


def test_simple_allocation(resource_catalog):
    """Test the return from a random allocation scheduling, using the specified catalog"""

    server, catalog = resource_catalog
    scheduler = wcase.WorstCase()
    start_time = dt.datetime.now()
    req = request.StorageRequest(capacity=20, duration=dt.timedelta(hours=3), start_time=start_time)

    server_id, target_node, target_disk = scheduler.compute(catalog, req)
    assert server_id == server
    assert 0 <= target_node < 2
    if target_node == 0:
        assert 0 <= target_disk < 2
    else:
        assert 0 <= target_disk < 3


def test_big_allocation(resource_catalog):
    """Try to allocate a storage space that will fit on one of the disks only"""

    server, catalog = resource_catalog
    scheduler = wcase.WorstCase()
    start_time = dt.datetime.now()
    req = request.StorageRequest(
        capacity=6500, duration=dt.timedelta(hours=3), start_time=start_time
    )
    server_id, target_node, target_disk = scheduler.compute(catalog, req)
    assert server_id == server
    assert target_node == 1
    assert target_disk == 1


def test_compute_allocation_overlap(resource_catalog):
    """Test the private function _compute_allocation_overlap, in charge of taking
    into account previous allocations on a disk and update a model of the disk/node
    bandwitdh accordingly if they overlap with new request

    This is a very badly written test, but we need to check how the algo react when adding
    more and more allocations on the disk prior to running it.
    """

    server, catalog = resource_catalog
    scheduler = wcase.WorstCase()
    start_time = dt.datetime.now()
    req = request.StorageRequest(
        capacity=500, duration=dt.timedelta(hours=3), start_time=start_time
    )

    # No previous allocation on disk:
    (
        offset,
        node_bw,
        disk_bw,
    ) = scheduler._WorstCase__compute_allocation_overlap(  # pylint: disable=no-member
        catalog.get_node(server, 0).disks[0], catalog.get_node(server, 0), req
    )

    assert offset == 0
    assert node_bw == 0
    assert disk_bw == 0

    # Existing (non conflicting) allocation, starting AFTER our new request):
    alloc_1 = request.StorageRequest(
        capacity=500,
        duration=dt.timedelta(hours=3),
        start_time=start_time + dt.timedelta(hours=3, minutes=30),
    )
    catalog.add_allocation(server, 0, 0, alloc_1)

    (
        offset,
        node_bw,
        disk_bw,
    ) = scheduler._WorstCase__compute_allocation_overlap(  # pylint: disable=no-member
        catalog.get_node(server, 0).disks[0], catalog.get_node(server, 0), req
    )

    assert offset == 0
    assert node_bw == 0
    assert disk_bw == 0

    # Existing (non conflicting) allocation, starting BEFORE our new request):
    alloc_2 = request.StorageRequest(
        capacity=500,
        duration=dt.timedelta(hours=3),
        start_time=start_time - dt.timedelta(hours=3, minutes=30),
    )
    catalog.add_allocation(server, 0, 0, alloc_2)

    (
        offset,
        node_bw,
        disk_bw,
    ) = scheduler._WorstCase__compute_allocation_overlap(  # pylint: disable=no-member
        catalog.get_node(server, 0).disks[0], catalog.get_node(server, 0), req
    )

    assert offset == 0
    assert node_bw == 0
    assert disk_bw == 0

    # Existing (conflicting) allocation, starting AFTER our new request):
    # overlap time is 2 hours
    alloc_3 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=3),
        start_time=start_time + dt.timedelta(hours=1),
    )
    catalog.add_allocation(server, 0, 0, alloc_3)

    (
        offset,
        node_bw,
        disk_bw,
    ) = scheduler._WorstCase__compute_allocation_overlap(  # pylint: disable=no-member
        catalog.get_node(server, 0).disks[0], catalog.get_node(server, 0), req
    )
    assert offset == 2 * 60 * 60
    assert node_bw == 2 * 60 * 60 * catalog.get_node(server, 0).bandwidth / 3
    assert disk_bw == 2 * 60 * 60 * catalog.get_node(server, 0).disks[0].write_bandwidth / 3

    # Existing (conflicting) allocation, starting AFTER our new request and ending BEFORE:
    # overlap time is 1 hours
    alloc_4 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=1),
        start_time=start_time + dt.timedelta(hours=1, minutes=30),
    )
    catalog.add_allocation(server, 0, 0, alloc_4)

    (
        offset,
        node_bw,
        disk_bw,
    ) = scheduler._WorstCase__compute_allocation_overlap(  # pylint: disable=no-member
        catalog.get_node(server, 0).disks[0], catalog.get_node(server, 0), req
    )

    assert offset == 2 * 60 * 60 + 1 * 60 * 60
    assert (
        node_bw
        == 1 * 60 * 60 * catalog.get_node(server, 0).bandwidth / 4
        + 2 * 60 * 60 * catalog.get_node(server, 0).bandwidth / 3
    )
    assert (
        disk_bw
        == 1 * 60 * 60 * catalog.get_node(server, 0).disks[0].write_bandwidth / 4
        + 2 * 60 * 60 * catalog.get_node(server, 0).disks[0].write_bandwidth / 3
    )

    # Existing (conflicting) allocation, starting AFTER our new request and ending AFTER:
    # overlap time is 30 minutes
    alloc_5 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=1),
        start_time=start_time + dt.timedelta(hours=2, minutes=30),
    )
    catalog.add_allocation(server, 0, 0, alloc_5)

    (
        offset,
        node_bw,
        disk_bw,
    ) = scheduler._WorstCase__compute_allocation_overlap(  # pylint: disable=no-member
        catalog.get_node(server, 0).disks[0], catalog.get_node(server, 0), req
    )

    assert offset == 2 * 60 * 60 + 1 * 60 * 60 + 0.5 * 60 * 60
    assert (
        node_bw
        == 1 * 60 * 60 * catalog.get_node(server, 0).bandwidth / 5
        + 0.5 * 60 * 60 * catalog.get_node(server, 0).bandwidth / 4
        + 2 * 60 * 60 * catalog.get_node(server, 0).bandwidth / 3
    )
    assert (
        disk_bw
        == 1 * 60 * 60 * catalog.get_node(server, 0).disks[0].write_bandwidth / 5
        + 0.5 * 60 * 60 * catalog.get_node(server, 0).disks[0].write_bandwidth / 4
        + 2 * 60 * 60 * catalog.get_node(server, 0).disks[0].write_bandwidth / 3
    )


def test_compute_status(resource_catalog):
    """Test private function _compute_status, which should fill disk_status and node_status
    values in our resource catalog, based on the already allocated requests.
    In the end, the bandwidth values obtained will be used to model which node/disk pair is
    most suited to receive the new allocation request
    """

    server, catalog = resource_catalog
    scheduler = wcase.WorstCase()
    # All allocations will, by default, have already started in our test
    start_time = dt.datetime.now() - dt.timedelta(minutes=20)

    # Add a few allocations in our resource catalog
    alloc_1 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=2),
        start_time=start_time,
    )
    alloc_2 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=1),
        start_time=start_time,
    )
    alloc_3 = request.StorageRequest(  # Already over (shouldn't appear in real cases)
        capacity=100,
        duration=dt.timedelta(minutes=15),
        start_time=start_time,
    )
    alloc_4 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=2),
        start_time=start_time + dt.timedelta(minutes=40),
    )
    alloc_5 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=2),
        start_time=start_time + dt.timedelta(minutes=40),
    )
    alloc_6 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=1),
        start_time=start_time + dt.timedelta(minutes=45),
    )
    alloc_7 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=1),
        start_time=start_time + dt.timedelta(minutes=20),
    )
    catalog.add_allocation(server, 0, 0, alloc_1)  # Node 0, Disk 0, -0:20 -> 1:40
    catalog.add_allocation(server, 0, 1, alloc_2)  # Node 0, Disk 1, -0:20 -> 0:40
    catalog.add_allocation(server, 1, 0, alloc_3)  # Node 1, Disk 0  -0:20 -> -0:15
    catalog.add_allocation(server, 1, 1, alloc_4)  # Node 1, Disk 1   0:20 -> 2:20
    catalog.add_allocation(server, 1, 2, alloc_5)  # Node 1, Disk 1   0:20 -> 2:20
    catalog.add_allocation(server, 0, 1, alloc_6)  # Node 0, Disk 1 (again) 0:45 -> 1:45
    catalog.add_allocation(server, 1, 1, alloc_7)  # Node 1, Disk 1 (again) 0:00 -> 1:00

    # Storage requests that overlaps at some point with every existing allocation, except alloc_3
    req = request.StorageRequest(
        capacity=500,
        duration=dt.timedelta(hours=3),
        start_time=start_time + dt.timedelta(minutes=20),
    )

    scheduler._WorstCase__compute_status(catalog, req)  # pylint: disable=no-member

    # And now for some asserts:
    # First, every calculated bandwidth should be positive, and inferior or equal to the maximum
    # hardware bandwidth.
    for _, node in catalog.list_nodes():
        assert 0 < node.node_status.bandwidth <= node.bandwidth
        for disk in node.disks:
            assert 0 < disk.disk_status.bandwidth <= disk.write_bandwidth

    # Then, we want to ensure correct values are computed for specific allocations
    # (with values from a supposed working state as baseline)

    # Same node status bandwidth for both nodes as there are the same max number of concurrent allocations for each
    # (N1 has 3 disks, but one of them as no active allocations)
    assert round(catalog.get_node(server, 0).node_status.bandwidth, 3) == 8.796
    assert round(catalog.get_node(server, 1).node_status.bandwidth, 3) == 8.796
    # No currently active allocation on Disk 0 from node 1, our request shall have the entire disk bandwidth.
    assert round(catalog.get_node(server, 1).disks[0].disk_status.bandwidth, 3) == 2.93
    # no other disk could have a computed bandwidth of 2.93 (max)
    for _, node, disk in catalog.list_resources():
        if node.uid != 1 and disk.uid != 0:
            assert disk.disk_status.bandwidth != 2.93
    # On node 1, disk 1 has more allocations than disk 2, so it's bandwidth should be reduced
    assert (
        catalog.get_node(server, 1).disks[1].disk_status.bandwidth
        < catalog.get_node(server, 1).disks[2].disk_status.bandwidth
    )


def test_compute(resource_catalog):
    """Testing the correct choice of disk, once bandwidth are computed"""

    # Same setup as previous test
    server, catalog = resource_catalog
    scheduler = wcase.WorstCase()
    # All allocations will, by default, have already started in our test
    start_time = dt.datetime.now() - dt.timedelta(minutes=20)

    # Add a few allocations in our resource catalog
    alloc_1 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=2),
        start_time=start_time,
    )
    alloc_2 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=1),
        start_time=start_time,
    )
    alloc_3 = request.StorageRequest(  # Already over (shouldn't appear in real cases)
        capacity=100,
        duration=dt.timedelta(minutes=15),
        start_time=start_time,
    )
    alloc_4 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=2),
        start_time=start_time + dt.timedelta(minutes=40),
    )
    alloc_5 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=2),
        start_time=start_time + dt.timedelta(minutes=40),
    )
    alloc_6 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=1),
        start_time=start_time + dt.timedelta(minutes=45),
    )
    alloc_7 = request.StorageRequest(
        capacity=100,
        duration=dt.timedelta(hours=1),
        start_time=start_time + dt.timedelta(minutes=20),
    )
    catalog.add_allocation(server, 0, 0, alloc_1)  # Node 0, Disk 0, -0:20 -> 1:40
    catalog.add_allocation(server, 0, 1, alloc_2)  # Node 0, Disk 1, -0:20 -> 0:40
    catalog.add_allocation(server, 1, 0, alloc_3)  # Node 1, Disk 0  -0:20 -> -0:15
    catalog.add_allocation(server, 1, 1, alloc_4)  # Node 1, Disk 1   0:20 -> 2:20
    catalog.add_allocation(server, 1, 2, alloc_5)  # Node 1, Disk 1   0:20 -> 2:20
    catalog.add_allocation(server, 0, 1, alloc_6)  # Node 0, Disk 1 (again) 0:45 -> 1:45
    catalog.add_allocation(server, 1, 1, alloc_7)  # Node 1, Disk 1 (again) 0:00 -> 1:00

    # Storage requests that overlaps at some point with every existing allocation, except alloc_3
    req = request.StorageRequest(
        capacity=500,
        duration=dt.timedelta(hours=3),
        start_time=start_time + dt.timedelta(minutes=20),
    )

    (server_r, node_r, disk_r) = scheduler.compute(catalog, req)
    assert server_r == server  # Not much of a choice
    assert node_r == 1
    assert disk_r == 0
