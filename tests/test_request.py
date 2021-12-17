""" Tests for storalloc.request
"""

import datetime as dt

import pytest

from storalloc import request as rq


def test_request_creation():
    """Test the creation of a new requests and validate initial state"""

    with pytest.raises(TypeError):
        rq.StorageRequest()

    start_time = dt.datetime.now()
    req = rq.StorageRequest(capacity=20, duration=dt.timedelta(hours=3), start_time=start_time)

    assert req.capacity == 20
    assert req.duration == dt.timedelta(hours=3)
    assert req.end_time == start_time + dt.timedelta(hours=3)
    assert req.client_id == ""
    assert req.job_id == ""
    assert req.node_id == 0
    assert req.disk_id == 0
    assert req.server_id == ""
    assert req.alloc_type == ""
    assert req.nqn == ""
    assert req.state == rq.ReqState.OPENED
    assert req.reason == ""

    # Capacity value is incorrect
    with pytest.raises(ValueError):
        rq.StorageRequest(capacity=0, duration=dt.timedelta(hours=3), start_time=start_time)

    with pytest.raises(ValueError):
        rq.StorageRequest(capacity=-3, duration=dt.timedelta(hours=3), start_time=start_time)

    # Duration value is incorrect
    with pytest.raises(ValueError):
        rq.StorageRequest(capacity=20, duration=dt.timedelta(hours=0), start_time=start_time)

    with pytest.raises(ValueError):
        rq.StorageRequest(capacity=20, duration=dt.timedelta(hours=-3), start_time=start_time)


def test_request_serialisation():
    """Serialise / deserialise a request"""

    # Initial request, as created in previous test
    start_time = dt.datetime.now()
    req_a = rq.StorageRequest(capacity=20, duration=dt.timedelta(hours=3), start_time=start_time)

    schema = rq.RequestSchema()

    serialised = schema.dump(req_a)
    assert serialised["capacity"] == 20
    assert serialised["duration"] == dt.timedelta(hours=3).total_seconds()
    assert serialised["start_time"] == start_time.isoformat()
    assert serialised["end_time"] == (start_time + dt.timedelta(hours=3)).isoformat()
    assert serialised["state"] == rq.ReqState.OPENED.value

    req_b = schema.load(serialised)
    assert req_b == req_a  # means that their end_time is equal
    assert req_b.capacity == req_a.capacity
    assert req_b.duration == req_a.duration
    assert req_b.start_time == req_a.start_time
    assert req_b.end_time == req_a.end_time
    assert req_b.state == req_a.state

    assert req_a == req_b


def test_request_overdue():
    """Check if a request is overdue or not (end_time is earlier than current time)"""

    # Overdue request
    start_time = dt.datetime.now() - dt.timedelta(minutes=5)
    req_a = rq.StorageRequest(capacity=20, duration=dt.timedelta(minutes=3), start_time=start_time)
    assert req_a.is_overdue()

    # Not overdue request
    req_b = rq.StorageRequest(capacity=20, duration=dt.timedelta(minutes=10), start_time=start_time)
    assert not req_b.is_overdue()

    # req_a wasn't overdue 4 minutes ago
    assert not req_a.is_overdue(current_time=(dt.datetime.now() - dt.timedelta(minutes=4)))

    # req_b will be overdue in 11 minutes
    assert req_b.is_overdue(current_time=(dt.datetime.now() + dt.timedelta(minutes=11)))


def test_request_overlap():
    """Check if two requests overlap for some of their execution

    t --5-------10-----20-22----30---35---37-40------50----------------->
    A -----------------[-+-+-+-+-+-+-+-+-+-+-]---------------...
    B --[-+-+-+-]--------------------------------------------...
    C ----------[-+-+-+-+-+-+-+-]----------------------------...
    D --------------------[+-+-+-+-+-]-----------------------...
    E ------------------------------------[+-+-+-+-+-]-------...

    """

    # Common start time
    start_time = dt.datetime.now()

    req_a = rq.StorageRequest(
        capacity=20,
        duration=dt.timedelta(minutes=20),
        start_time=start_time + dt.timedelta(minutes=20),
    )
    req_b = rq.StorageRequest(
        capacity=20,
        duration=dt.timedelta(minutes=5),
        start_time=start_time + dt.timedelta(minutes=5),
    )
    req_c = rq.StorageRequest(
        capacity=20,
        duration=dt.timedelta(minutes=20),
        start_time=start_time + dt.timedelta(minutes=10),
    )
    req_d = rq.StorageRequest(
        capacity=20,
        duration=dt.timedelta(minutes=13),
        start_time=start_time + dt.timedelta(minutes=22),
    )
    req_e = rq.StorageRequest(
        capacity=20,
        duration=dt.timedelta(minutes=13),
        start_time=start_time + dt.timedelta(minutes=37),
    )

    assert req_a.overlaps(req_d) == req_d.duration.total_seconds()
    assert req_a.overlaps(req_c) == (req_c.end_time - req_a.start_time).total_seconds()
    assert req_a.overlaps(req_c) == req_c.overlaps(req_a)
    assert req_a.overlaps(req_e) == (req_a.end_time - req_e.start_time).total_seconds()
    assert req_a.overlaps(req_b) == 0.0
    assert req_b.overlaps(req_c) == 0.0
