"""Queue manager process"""

from multiprocessing import Process
import datetime
from collections import deque

import zmq

from storalloc.utils.logging import get_storalloc_logger
from storalloc.utils.transport import Transport
from storalloc.utils.message import Message, MsgCat
from storalloc.request import RequestSchema, ReqState


class AllocationQueue(Process):
    """Class responsible for interacting with a request deque"""

    def __init__(self, uid: str, verbose: bool = True):
        """Init"""
        super().__init__()
        self.uid = uid
        self.log = get_storalloc_logger(verbose)

    def _store_request(self, new):
        """Store a request ordered by storage allocation walltime inside the inner deque"""

        insert_idx = 0
        for request in reversed(self.request_deque):
            if request <= new:
                break
            insert_idx += 1

        # Insertion is slower than append for deque. Here we assume it's worth checking if
        # we can append instead of insert, so that we don't need to get the length of the deque
        # and can use the faster 'append', but it might also not be worth it at all
        # TODO bench-fucking-mark it.
        if insert_idx:
            self.request_deque.insert_back(len(self.request_deque) - insert_idx, new)
        else:
            self.request_deque.append(new)

    def _prune_requests(self):
        """Prune request from deque if their end_time is overdue
        Request are inserted by end_time ordering, so once we reach a non-overdue
        request, all following requests are still valid as well.
        """

        current_time = datetime.datetime.now()

        while self.request_deque and self.request_deque[0].is_overdue(current_time):
            request = self.request_deque.popleft()
            request.state = ReqState.ENDED
            self.transport.send_multipart(Message(MsgCat.REQUEST, request))

    def run(self):
        """Start an infinite loop with two objectives :
        - wake up every few seconds and possibly clean up overdue allocations from queue,
            then send a message to inform the orchestrator
        - receive processed requests from orchestrator and keep track of them.
        """

        self.request_deque = deque()
        self.context = zmq.Context()
        socket = self.context.socket(zmq.DEALER)  # pylint: disable=no-member
        socket.setsockopt(zmq.IDENTITY, self.uid.encode("utf-8"))  # pylint: disable=no-member
        socket.connect("ipc://queue_manager.ipc")
        self.transport = Transport(socket)
        self.schema = RequestSchema()

        self.transport.send_multipart(Message.notification("queue_manager-alive"))

        while True:

            event = self.transport.socket.poll(timeout=5000)

            # Start by pruning old requests.
            self._prune_requests()

            if event:
                _, message = self.transport.recv_multipart()
                if message.category == MsgCat.REQUEST:
                    request = self.schema.load(message.content)
                    self._store_request(request)
                elif message.category == MsgCat.NOTIFICATION:
                    # Answer to keep alive messages from router
                    notification = Message.notification("keep-alive")
                    self.transport.send_multipart(notification)
