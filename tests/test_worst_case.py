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
