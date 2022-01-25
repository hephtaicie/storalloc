""" Testing round robin scheduling strategy
"""

import pytest

from storalloc import resources
from storalloc.strategies import round_robin as rr


@pytest.fixture
def catalog():
    """Return a resource catalog and server_id"""

    rcatalog = resources.ResourceCatalog()
    rcatalog.nodes_from_yaml("S-15362", "tests/dummy_system.yml")
    rcatalog.nodes_from_yaml("S-89214", "tests/dummy_system_2.yml")
    return rcatalog


def test_round_robin(catalog):
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
