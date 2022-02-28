""" Tests for storalloc.orchestrator.queue
"""

import random
import logging
import datetime as dt
import pytest
import zmq

from storalloc.orchestrator.queue import AllocationQueue
from storalloc.request import StorageRequest, RequestSchema, ReqState
from storalloc.utils.message import Message, MsgCat
from storalloc.utils.transport import Transport

# pylint: disable=redefined-outer-name,missing-function-docstring,protected-access


@pytest.fixture(name="zmqctx")
def context():
    context = zmq.Context()
    yield context
    context.term()


@pytest.fixture(name="queue")
def alloc_queue():
    uid = "test_alloc_queue"
    queue = AllocationQueue(uid, verbose=False)
    yield queue
    if queue.is_alive():
        queue.terminate()


@pytest.fixture
def rand_request():
    req = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=random.randint(5, 7)),
        start_time=dt.datetime.now(),
    )
    return req


@pytest.fixture
def schema():
    return RequestSchema()


@pytest.fixture
def rand_message(rand_request, schema):
    msg = Message(category=MsgCat.REQUEST, content=schema.dump(rand_request))
    return msg


def test_start_queue(zmqctx, queue):
    """Start a queue process and check connectivity"""
    socket = zmqctx.socket(zmq.ROUTER)  # pylint: disable=no-member
    socket.bind("ipc://queue_manager.ipc")  # nasty constant in code...
    queue.start()
    assert socket.poll(timeout=4000)


def test_request_basic(zmqctx, queue, rand_message, schema):
    """Test receiving and handling a basic request (not splitted)"""
    router = zmqctx.socket(zmq.ROUTER)  # pylint: disable=no-member
    router.bind("ipc://queue_manager.ipc")  # nasty constant in code...
    queue.start()
    transport = Transport(router)
    assert transport.poll(timeout=3000)
    identities, message = transport.recv_multipart()
    assert message.category == MsgCat.NOTIFICATION

    # send request, which will live no more than 5s, and wait for deallocation message
    transport.send_multipart(rand_message, "test_alloc_queue")
    identities, message = transport.recv_multipart()
    assert "test_alloc_queue" in identities
    assert message.category == MsgCat.REQUEST
    request = schema.load(message.content)
    assert request.state == ReqState.ENDED


def test_store_request():
    """Test the store_request method"""

    uid = "test_alloc_queue"
    queue = AllocationQueue(uid, verbose=False)

    req1 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=30),
        start_time=dt.datetime.now(),
    )
    req2 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=40),
        start_time=dt.datetime.now(),
    )
    req3 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=50),
        start_time=dt.datetime.now(),
    )
    req4 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=55),
        start_time=dt.datetime.now(),
    )

    queue._AllocationQueue__store_request(req1)
    queue._AllocationQueue__store_request(req2)
    queue._AllocationQueue__store_request(req3)
    queue._AllocationQueue__store_request(req4)
    assert len(queue.request_deque) == 4
    assert queue.request_deque[0] == req1
    assert queue.request_deque[3] == req4


def test_store_request_reverse():
    """Test the store_request method"""

    uid = "test_alloc_queue"
    queue = AllocationQueue(uid, verbose=False)

    req1 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=55),
        start_time=dt.datetime.now(),
    )
    req2 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=22),
        start_time=dt.datetime.now(),
    )
    req3 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=34),
        start_time=dt.datetime.now(),
    )
    req4 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=16),
        start_time=dt.datetime.now(),
    )

    queue._AllocationQueue__store_request(req1)
    queue._AllocationQueue__store_request(req2)
    queue._AllocationQueue__store_request(req3)
    queue._AllocationQueue__store_request(req4)
    assert len(queue.request_deque) == 4
    assert queue.request_deque[0] == req4
    assert queue.request_deque[1] == req2
    assert queue.request_deque[2] == req3
    assert queue.request_deque[3] == req1


def test_store_request_splitted():
    """Test the store_request method with a splitted request"""

    uid = "test_alloc_queue"
    queue = AllocationQueue(uid, verbose=False)

    req1 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=55),
        start_time=dt.datetime.now(),
    )
    req1.divided = 3
    # 23rd request group handled by fake orchestrator, 3 part (0, 1, 2) of splitted group
    req1.job_id = "C1234-23-2"

    queue._AllocationQueue__store_request(req1)
    assert queue.split_requests["C1234-23"] == {"ttl": 103, "due_parts": 2, "requests": [req1]}

    req2 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=55),
        start_time=dt.datetime.now(),
    )
    req2.divided = 3
    # 23rd request group handled by fake orchestrator, 3 part (0, 1, 2) of splitted group
    req2.job_id = "C1234-23-3"

    queue._AllocationQueue__store_request(req2)
    assert queue.split_requests["C1234-23"] == {
        "ttl": 103,  # in normal operation, the ttl should be 102, but we're testing in isolation
        "due_parts": 1,
        "requests": [req1, req2],
    }


def test_prune_requests(zmqctx, schema):
    """Test the __prune_requests private method
    (quite complexe out of its process, as it requires
    an operational ZMQ Dealer)
    """

    # Prepare communication
    router = zmqctx.socket(zmq.ROUTER)  # pylint: disable=no-member
    router.setsockopt(zmq.IDENTITY, "test_router".encode("utf-8"))  # pylint: disable=no-member
    router.bind("ipc://queue_manager.ipc")  # nasty constant in code...
    router_t = Transport(router)
    dealer = zmqctx.socket(zmq.DEALER)  # pylint: disable=no-member
    dealer.setsockopt(zmq.IDENTITY, "test_dealer".encode("utf-8"))  # pylint: disable=no-member
    dealer.connect("ipc://queue_manager.ipc")  # nasty constant in code...

    # Instanciate AllocationQueue and setup a transport
    uid = "test_alloc_queue"
    log = logging.getLogger("test_queue")
    queue = AllocationQueue(uid, verbose=False)
    queue.transport = Transport(dealer)
    queue.log = log

    # Add a few requests to the queue, two of them being overdue
    req1 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=55),
        start_time=dt.datetime.now() - dt.timedelta(minutes=3),
    )
    req1.job_id = "REQ1"
    req2 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=22),
        start_time=dt.datetime.now() - dt.timedelta(minutes=2),
    )
    req2.job_id = "REQ2"
    req3 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=34),
        start_time=dt.datetime.now(),
    )
    req3.job_id = "REQ3"
    queue._AllocationQueue__store_request(req1)
    queue._AllocationQueue__store_request(req2)
    queue._AllocationQueue__store_request(req3)

    # Call the __prune_requests method and wait for 2 messages (one per ended request)
    queue._AllocationQueue__prune_requests()
    # First request ended
    _, message = router_t.recv_multipart()
    assert message.category == MsgCat.REQUEST
    req = schema.load(message.content)
    assert req.state == ReqState.ENDED
    assert req.job_id == "REQ1"
    # Second request ended
    _, message = router_t.recv_multipart()
    assert message.category == MsgCat.REQUEST
    req = schema.load(message.content)
    assert req.state == ReqState.ENDED
    assert req.job_id == "REQ2"

    # No more messages from pruning method
    assert not router_t.poll(timeout=1000)
    assert len(queue.request_deque) == 1


def test_check_split(zmqctx, schema):
    """Check split request"""

    # Prepare communication
    router = zmqctx.socket(zmq.ROUTER)  # pylint: disable=no-member
    router.setsockopt(zmq.IDENTITY, "test_router".encode("utf-8"))  # pylint: disable=no-member
    router.bind("ipc://queue_manager.ipc")  # nasty constant in code...
    router_t = Transport(router)
    dealer = zmqctx.socket(zmq.DEALER)  # pylint: disable=no-member
    dealer.setsockopt(zmq.IDENTITY, "test_dealer".encode("utf-8"))  # pylint: disable=no-member
    dealer.connect("ipc://queue_manager.ipc")  # nasty constant in code...

    # Instanciate AllocationQueue and setup a transport
    uid = "test_alloc_queue"
    log = logging.getLogger("test_queue")
    queue = AllocationQueue(uid, verbose=False)
    queue.transport = Transport(dealer)
    queue.log = log

    # Faking a correct split_requests dict (values in 'requests' fields are incorrect)
    req1 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=55),
        start_time=dt.datetime.now(),
        job_id="REQ1",
    )
    req2 = StorageRequest(
        capacity=random.uniform(15, 1024),
        duration=dt.timedelta(seconds=55),
        start_time=dt.datetime.now(),
        job_id="REQ2",
    )

    queue.split_requests = {
        "A": {"ttl": 0, "due_parts": 1, "requests": [req1, req2]},
        "B": {"ttl": 10, "due_parts": 1, "requests": [req2, req1]},
        "C": {"ttl": 0, "due_parts": 0, "requests": [req1, req2]},
    }

    queue._AllocationQueue__check_splits()

    assert router_t.poll(timeout=1000)
    _, message = router_t.recv_multipart()
    assert message.category == MsgCat.REQUEST
    req = schema.load(message.content)
    assert req.state == ReqState.ENDED
    assert req.job_id == "REQ1"
    # Second request ended
    assert router_t.poll(timeout=1000)
    _, message = router_t.recv_multipart()
    assert message.category == MsgCat.REQUEST
    req = schema.load(message.content)
    assert req.state == ReqState.ENDED
    assert req.job_id == "REQ2"

    # No more messages from pruning method
    assert not router_t.poll(timeout=1000)
    assert "A" not in queue.split_requests
