""" Tests for storalloc.orchestrator.scheduler
"""

import random
import logging
import datetime as dt
import pytest
import zmq

from storalloc.orchestrator.scheduler import Scheduler
from storalloc.request import StorageRequest, RequestSchema, ReqState
from storalloc.utils.message import Message, MsgCat
from storalloc.utils.transport import Transport


@pytest.fixture(name="zmqctx")
def context():
    """ZMQ context"""
    context = zmq.Context()
    yield context
    context.term()


@pytest.fixture(name="sched")
def scheduler():
    """Start and kill a scheduler process"""
    uid = "test_scheduler"
    scheduler = Scheduler(uid, verbose=False)
    yield scheduler
    if scheduler.is_alive():
        scheduler.terminate()


@pytest.fixture
def rand_request():
    """Return a random allocation request"""
    req = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=random.randint(5, 7)),
        start_time=dt.datetime.now(),
    )
    return req


@pytest.fixture
def schema():
    """Return a RequestSchema object (not very useful as a fixture...)"""
    return RequestSchema()


@pytest.fixture
def rand_message(rand_request, schema):
    """Return a Message containing a random allocation request"""
    msg = Message(category=MsgCat.REQUEST, content=schema.dump(rand_request))
    return msg


def test_start_scheduler(zmqctx, sched):
    """Start a scheduler process and check that it's alive"""

    router = zmqctx.socket(zmq.ROUTER)  # pylint: disable=no-member
    router.bind("ipc://scheduler.ipc")  # nasty constant in code...
    sched.start()
    assert router.poll(timeout=4000)
