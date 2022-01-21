"""Queue manager process"""

from multiprocessing import Process
import datetime
from collections import deque

import zmq

from storalloc.utils.logging import get_storalloc_logger, add_remote_handler
from storalloc.utils.transport import Transport
from storalloc.utils.message import Message, MsgCat
from storalloc.request import RequestSchema, ReqState


class AllocationQueue(Process):
    """Class responsible for interacting with a request deque"""

    def __init__(self, uid: str, verbose: bool = False, remote_logging: tuple = None):
        """Init"""
        super().__init__()
        self.uid = uid
        self.verbose = verbose
        self.log = None  # Has to be set up on process start in run()
        self.remote_logging = remote_logging
        self.polling_time = 1000  # ms

        self.schema = RequestSchema()
        self.request_deque = deque()
        self.split_requests = {}
        self.transport = None

    def __store_request(self, new):
        """Store a request ordered by storage allocation walltime inside the inner deque"""

        insert_idx = 0
        for request in reversed(self.request_deque):
            if request.end_time <= new.end_time:
                break
            insert_idx += 1

        # Insertion is slower than append for deque. Here we assume it's worth checking if
        # we can append instead of insert, so that we don't need to get the length of the deque
        # and can use the faster 'append', but it might also not be worth it at all
        # TODO benchmark it.
        if insert_idx:
            self.request_deque.insert(len(self.request_deque) - insert_idx, new)
        else:
            self.request_deque.append(new)

        if new.divided > 1:
            req_id = new.job_id.rsplit("-", 1)[0]
            if req_id not in self.split_requests:
                self.split_requests[req_id] = {
                    "ttl": 100 + new.divided,
                    "due_parts": new.divided - 1,
                    "requests": [new],
                }
            else:
                self.split_requests[req_id]["due_parts"] -= 1
                self.split_requests[req_id]["requests"].append(new)

    def __prune_requests(self):
        """Prune request from deque if their end_time is overdue
        Request are inserted by end_time ordering, so once we reach a non-overdue
        request, all following requests are still valid as well.
        """

        current_time = datetime.datetime.now()

        while self.request_deque and self.request_deque[0].is_overdue(current_time):
            request = self.request_deque.popleft()
            request.state = ReqState.ENDED
            self.log.debug(f"Deallocating request {request.job_id}")
            self.transport.send_multipart(Message(MsgCat.REQUEST, self.schema.dump(request)))

    def __check_splits(self):
        """Loop through every split requests accounted for, and ensure none of them are
        incomplete after a given time threshold
        """
        removed = []
        for req_id, info in self.split_requests.items():
            info["ttl"] -= 1
            if info["ttl"] <= 0 and info["due_parts"] > 0:
                for request in info["requests"]:
                    request.state = ReqState.ENDED
                    request.reason = "TTL exceeded before receiving all parts of splitted requests"
                    self.transport.send_multipart(
                        Message(MsgCat.REQUEST, self.schema.dump(request))
                    )
                removed.append(req_id)

        for req_id in removed:
            del self.split_requests[req_id]

    # Covered by functional tests, but not visible in coverage report as it is tested from
    # a forked process
    def run(self):  # pragma: no cover
        """Start an infinite loop with two objectives :
        - wake up every few seconds and possibly clean up overdue allocations from queue,
            then send a message to inform the orchestrator
        - receive processed requests from orchestrator and keep track of them.
        """

        context = zmq.Context()
        socket = context.socket(zmq.DEALER)  # pylint: disable=no-member
        socket.setsockopt(zmq.IDENTITY, self.uid.encode("utf-8"))  # pylint: disable=no-member
        socket.connect("ipc://queue_manager.ipc")
        self.transport = Transport(socket)

        self.log = get_storalloc_logger(self.verbose, True, self.uid)
        if self.remote_logging:
            add_remote_handler(
                self.log, self.uid, context, self.remote_logging[0], self.remote_logging[1]
            )

        self.transport.send_multipart(Message.notification("queue_manager-alive"))

        while True:

            event = self.transport.socket.poll(timeout=5000)

            # Start by pruning old requests.
            self.__prune_requests()

            if event:
                _, message = self.transport.recv_multipart()
                if message.category == MsgCat.REQUEST:
                    request = self.schema.load(message.content)
                    self.log.debug(f"Received new request {request.job_id}, processing now...")
                    self.__store_request(request)
                elif message.category == MsgCat.NOTIFICATION:
                    # Answer to keep alive messages from router
                    notification = Message.notification("keep-alive")
                    self.transport.send_multipart(notification)

            self.__check_splits()
