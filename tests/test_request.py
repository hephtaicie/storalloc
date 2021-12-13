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
    assert req.client_id == 0
    assert req.job_id == ""
    assert req.node_id == 0
    assert req.disk_id == 0
    assert req.server_id == ""
    assert req.alloc_type == ""
    assert req.nqn == ""
    assert req.state == rq.ReqState.OPENED
    assert req.reason == ""


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
