""" Testing round robin scheduling strategy
"""

import datetime as dt

import pytest

from storalloc import resources
from storalloc.strategies import round_robin as rr
from storalloc import request as rq


@pytest.fixture
def catalog():
    """Return a resource catalog and server_id"""

    rcatalog = resources.ResourceCatalog()
    rcatalog.nodes_from_yaml("S-15362", "tests/dummy_system.yml")
    rcatalog.nodes_from_yaml("S-89214", "tests/dummy_system_2.yml")
    return rcatalog


def test_no_request_given(catalog):
    """Test round robin scheduling algorithm"""

    scheduler = rr.RoundRobin()
    expected = [
        ("S-15362", 0, 0),
        ("S-89214", 0, 0),
        ("S-15362", 1, 0),
        ("S-89214", 1, 0),
        ("S-15362", 0, 1),
        ("S-89214", 2, 0),
        ("S-15362", 1, 1),
        ("S-89214", 0, 1),
        ("S-15362", 0, 0),
        ("S-89214", 1, 1),
        ("S-15362", 1, 2),
        ("S-89214", 2, 0),
        ("S-15362", 0, 1),
        ("S-89214", 0, 2),
        ("S-15362", 1, 0),
        ("S-89214", 1, 2),
        ("S-15362", 0, 0),
        ("S-89214", 2, 0),
        ("S-15362", 1, 1),
        ("S-89214", 0, 0),
        ("S-15362", 0, 1),
        ("S-89214", 1, 3),
        ("S-15362", 1, 2),
        ("S-89214", 2, 0),
    ]

    for exp in expected:
        res = scheduler.compute(catalog)
        assert res == exp


def test_request_given(catalog):
    """Test round robin scheduling algorithm"""

    start_time = dt.datetime.now()
    req = rq.StorageRequest(capacity=20, duration=dt.timedelta(hours=3), start_time=start_time)

    scheduler = rr.RoundRobin()
    expected = [
        ("S-15362", 0, 0),
        ("S-89214", 0, 0),
        ("S-15362", 1, 0),
        ("S-89214", 1, 0),
        ("S-15362", 0, 1),
        ("S-89214", 2, 0),
        ("S-15362", 1, 1),
        ("S-89214", 0, 1),
        ("S-15362", 0, 0),
        ("S-89214", 1, 1),
        ("S-15362", 1, 2),
        ("S-89214", 2, 0),
        ("S-15362", 0, 1),
        ("S-89214", 0, 2),
        ("S-15362", 1, 0),
        ("S-89214", 1, 2),
        ("S-15362", 0, 0),
        ("S-89214", 2, 0),
        ("S-15362", 1, 1),
        ("S-89214", 0, 0),
        ("S-15362", 0, 1),
        ("S-89214", 1, 3),
        ("S-15362", 1, 2),
        ("S-89214", 2, 0),
    ]

    for exp in expected:
        res = scheduler.compute(catalog, req)
        assert res == exp


def test_request_unfit_for_disks(catalog):
    """Test round robin algorithm with a request too big to fit any of the disks"""

    start_time = dt.datetime.now()
    req = rq.StorageRequest(capacity=12000, duration=dt.timedelta(hours=3), start_time=start_time)
    scheduler = rr.RoundRobin()
    res = scheduler.compute(catalog, req)
    assert res == ("", -1, -1)


def test_disks_already_full(catalog):
    """Test round robin algorithm when at least one disk which should have been selected
    is actually unfit due to previous allocation(s)"""

    start_time = dt.datetime.now()
    alloc_1 = rq.StorageRequest(
        capacity=3700,
        duration=dt.timedelta(hours=3, minutes=30),
        start_time=start_time + dt.timedelta(hours=1),
    )
    catalog.add_allocation("S-15362", 0, 0, alloc_1)
    catalog.add_allocation("S-89214", 0, 0, alloc_1)

    req = rq.StorageRequest(capacity=350, duration=dt.timedelta(hours=2), start_time=start_time)
    scheduler = rr.RoundRobin()
    res = scheduler.compute(catalog, req)
    assert res == ("S-15362", 1, 0)
