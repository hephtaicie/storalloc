""" Testing "random" scheduling strategy
"""

import datetime as dt
import pytest

from storalloc import resources
from storalloc import request
from storalloc.strategies import random_alloc as ra


@pytest.fixture
def resource_catalog():
    """Return a resource catalog and server_id"""

    server_id = "S-15362"
    return (server_id, resources.ResourceCatalog(server_id, "tests/dummy_system.yml"))


def test_random_alloc(resource_catalog):
    """Test the return from a random allocation scheduling, using the specified catalog"""

    server, catalog = resource_catalog
    scheduler = ra.RandomAlloc()
    start_time = dt.datetime.now()
    req = request.StorageRequest(capacity=20, duration=dt.timedelta(hours=3), start_time=start_time)
    for _ in range(100):
        server, target_node, target_disk = scheduler.compute(catalog, req)
        assert server == "S-15362"  # Not much of a choice
        assert 0 <= target_node < 2
        assert target_disk >= 0
        if target_node == 0:
            assert target_disk < 2
        else:
            assert target_disk < 3


def test_random_alloc_no_nodes():
    """Test the default return from "random" allocation when resource catalog has no node"""

    catalog = resources.ResourceCatalog()
    scheduler = ra.RandomAlloc()
    start_time = dt.datetime.now()
    req = request.StorageRequest(capacity=20, duration=dt.timedelta(hours=3), start_time=start_time)
    server, target_node, target_disk = scheduler.compute(catalog, req)
    assert server == ""
    assert target_node == target_disk == -1
