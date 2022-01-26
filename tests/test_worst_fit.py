""" Testing worst fit scheduling strategy
"""

import datetime as dt

import pytest

from storalloc import resources
from storalloc.strategies import worst_fit as wf
from storalloc import request as rq


@pytest.fixture
def catalog():
    """Return a resource catalog"""

    rcatalog = resources.ResourceCatalog()
    rcatalog.nodes_from_yaml("S-15362", "tests/dummy_system.yml")
    return rcatalog


@pytest.fixture
def sreq():
    start_time = dt.datetime.now()
    req = rq.StorageRequest(capacity=20, duration=dt.timedelta(hours=3), start_time=start_time)
    return req


def test_all_nodes_empty(catalog, sreq):
    """Test the worst fit algorithm when all nodes
    are 100% free (a disk at random will be chosen)"""

    scheduler = wf.WorstFit()
    res = scheduler.compute(catalog, sreq)
    assert res != ("", -1, -1)


def test_some_nodes_empty(catalog, sreq):
    """Test the worst fit algorithm when some nodes only are free"""

    # Add a few allocations on some of the disks
    start_time = dt.datetime.now()
    alloc_1 = rq.StorageRequest(
        capacity=3700,
        duration=dt.timedelta(hours=3, minutes=30),
        start_time=start_time + dt.timedelta(hours=1),
    )
    catalog.add_allocation("S-15362", 0, 0, alloc_1)
    catalog.add_allocation("S-15362", 0, 1, alloc_1)

    # Ensure request is allocated on second node, as the first one has allocations on
    # all of its disks
    scheduler = wf.WorstFit()
    res = scheduler.compute(catalog, sreq)
    assert res[0] == "S-15362"
    assert res[1] == 1


def test_all_disk_used(catalog, sreq):
    """Test the worst fit algorithm when there is a single best candidate among
    disks which all have allocations"""

    # Add a few allocations on some of the disks
    start_time = dt.datetime.now()
    alloc_1 = rq.StorageRequest(
        capacity=3700,
        duration=dt.timedelta(hours=3, minutes=30),
        start_time=start_time + dt.timedelta(hours=1),
    )
    catalog.add_allocation("S-15362", 0, 0, alloc_1)
    catalog.add_allocation("S-15362", 0, 1, alloc_1)
    catalog.add_allocation("S-15362", 1, 0, alloc_1)
    catalog.add_allocation("S-15362", 1, 1, alloc_1)  # thisdisk is 8TB (biggest in system)
    catalog.add_allocation("S-15362", 1, 2, alloc_1)

    # Ensure request is allocated on second node, as the first one has allocations on
    # all of its disks
    scheduler = wf.WorstFit()
    res = scheduler.compute(catalog, sreq)
    assert res == ("S-15362", 1, 1)
