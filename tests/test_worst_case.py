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
    assert server_id == "S-15362"
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
    assert server_id == "S-15362"
    assert target_node == 1
    assert target_disk == 1

