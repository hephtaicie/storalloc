"""Queue manager process"""

import datetime
from collections import deque

import zmq


class AllocationQueue:
    """Class responsible for interacting with a request deque"""

    def __init__(self):

        self.request_deque = deque()
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)  # pylint: disable=no-member
        self.socket.bind("ipc://queue_manager.ipc")

    def run(self):
        """Start an infinite loop with two objectives :
        - wake up every few seconds and possibly clean up overdue allocations from queue,
            then send a message to inform the orchestrator
        - receive processed requests from orchestrator and keep track of them.
        """

        while True:

            events = self.socket.poll(timeout=5000)

            if events:
                # Unpack message and store new request
                # self._store_request(new)
                pass

            # Even if we received nothing, check for overdue requests
            # self._prune_requests()

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
            self.request_deque.popleft()
