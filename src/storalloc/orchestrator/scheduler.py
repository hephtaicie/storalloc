""" Scheduler class

"""

from multiprocessing import Process
import datetime
import copy

import zmq

from storalloc.utils.logging import get_storalloc_logger, add_remote_handler
from storalloc.strategies.base import StrategyInterface
from storalloc.resources import ResourceCatalog, Node
from storalloc.utils.transport import Transport
from storalloc.utils.message import Message, MsgCat
from storalloc.request import RequestSchema, ReqState, StorageRequest


# pylint: disable=logging-not-lazy,logging-fstring-interpolation


class Scheduler(Process):
    """Scheduler class should run in its own process. It maintain an updated resource catalog
    made from the aggregation of 1..N resource catalog registered by storage server(s) and it
    runs a scheduling algorithm other this resource catalog in order to determine an optimal
    storage allocation placement for each request it receives.
    """

    def __init__(
        self,
        uid: str,
        strategy: StrategyInterface = None,
        allow_retry: bool = False,
        resource_catalog: ResourceCatalog = None,
        verbose: bool = False,
        remote_logging: tuple = None,
    ):
        """Init process"""
        super().__init__()
        self.uid = uid
        self.strategy = strategy
        self.allow_retry = allow_retry
        self.resource_catalog = resource_catalog
        self.schema = RequestSchema()
        self.ongoing_splits = {}
        self.transport = None  # waiting for context init to run when process is started
        self.log = None  # same
        self.verbose = verbose
        self.remote_logging = remote_logging

    def accumulate_request(self, request: StorageRequest):
        """
        If a request is split, register it in a buffer, and treat it when all parts are there.
        Otherwise, transfer to process_allocation_request immediately
        """

        if request.divided == 1:
            alloc_result = self.strategy.compute(self.resource_catalog, request)
            self.process_allocation_request(request, *alloc_result)
            return

        full_job_id = request.job_id
        last_dash = full_job_id.rfind("-")
        short_job_id = full_job_id[:last_dash]

        # Add entry
        if short_job_id not in self.ongoing_splits:
            self.ongoing_splits[short_job_id] = {full_job_id: [request]}
        # Update entry
        elif (
            short_job_id in self.ongoing_splits
            and (len(self.ongoing_splits[short_job_id]) + 1) < request.divided
        ):
            self.ongoing_splits[short_job_id][full_job_id] = [request]
        # Flush entry
        elif (
            short_job_id in self.ongoing_splits
            and (len(self.ongoing_splits[short_job_id]) + 1) == request.divided
        ):

            self.ongoing_splits[short_job_id][full_job_id] = [request]

            # Try to allocate every request
            all_allocated = True
            for req_id, value in self.ongoing_splits[short_job_id].items():
                alloc_res = self.strategy.compute(self.resource_catalog, value[0])
                if not alloc_res[0]:
                    self.log.error(
                        f"Unable to fulfill sub-request {req_id} / {value[0].capacity} GB"
                    )
                    all_allocated = False
                    break
                # Store scheduler results
                value.append(alloc_res)
                self.resource_catalog.add_allocation(*alloc_res, request)

            if all_allocated is False:

                for req_id, value in self.ongoing_splits[short_job_id].items():

                    # Remove allocation from resource catalog
                    if len(value) > 1:
                        self.resource_catalog.del_allocation(*value[1], value[0])

                    value[0].state = ReqState.REFUSED
                    value[0].reason = (
                        "Split request with at least one failure in allocating sub-requests. "
                        + "Delaying request not implemented for split requets"
                    )
                    # Send back the refused request to the orchestrator
                    # Maybe we could send as batch... ?
                    self.transport.send_multipart(
                        Message(MsgCat.REQUEST, self.schema.dump(value[0]))
                    )

                # Clean up and stop allocation here
                del self.ongoing_splits[short_job_id]
            else:
                # Every subrequest could be allocated, do it
                for _, value in self.ongoing_splits[short_job_id].items():
                    self.process_allocation_request(value[0], *value[1])
        else:
            print("###########################################################")
            print("############## SHOULD NEVER TRIGGER THIS ##################")
            print("###########################################################")
            raise ValueError("Triggered unknown state in accumulate_requests")

    def process_allocation_request(self, request, server_id, target_node, target_disk):
        """Run scheduling algo and attempt to find an optimal allocation for a given request"""

        self.log.debug(f"Processing PENDING allocation request : {request}")

        if server_id:
            request.node_id = target_node
            request.disk_id = target_disk
            request.server_id = server_id
            request.state = ReqState.GRANTED
            self.log.debug(
                f"Request {request.job_id} [GRANTED] on {server_id}:{target_node}:{target_disk}"
            )
            self.resource_catalog.add_allocation(server_id, target_node, target_disk, request)
            # Send back the allocated request to the orchestrator
            self.transport.send_multipart(Message(MsgCat.REQUEST, self.schema.dump(request)))
        elif self.allow_retry:
            if request.original_start_time is None:
                request.original_start_time = request.start_time

            if (request.start_time - request.original_start_time) < datetime.timedelta(
                hours=1
            ) and (request.start_time + datetime.timedelta(minutes=5) < request.end_time):
                request.start_time += datetime.timedelta(minutes=5)
                self.log.debug(f"Currently no resources for {request.job_id} : delaying by 5min")
                alloc_result = self.strategy.compute(self.resource_catalog, request)
                self.process_allocation_request(request, *alloc_result)
            else:
                self.log.error(
                    f"Unable to fulfill request {request.job_id} : "
                    f"{server_id}:{target_node}:{target_disk}"
                )
                request.state = ReqState.REFUSED
                request.reason = (
                    "Could not fit request onto current resources, even after delaying start"
                )
                # Send back the refused request to the orchestrator
                self.transport.send_multipart(Message(MsgCat.REQUEST, self.schema.dump(request)))
        else:
            self.log.error(
                f"Unable to fulfill request {request.job_id} : "
                f"{server_id}:{target_node}:{target_disk}"
            )
            request.state = ReqState.REFUSED
            request.reason = (
                "Could not fit request onto current resources/ Delaying start unallowed."
            )
            # Send back the refused request to the orchestrator
            self.transport.send_multipart(Message(MsgCat.REQUEST, self.schema.dump(request)))

    def process_deallocation_request(self, request):
        """Acknowledge the release of some storage resource in the resource catalog
        (request in ENDED state)"""
        # TODO: should actually remove allocation from disk in resource catalog
        self.log.debug(f"Processing ENDED request : {request}")

    def process_node_registration(self, server_id: str, node_data: dict):
        """Update resource catalog according to a new registration"""
        self.log.debug(f"Adding node data entry for from server [{server_id}]")
        self.resource_catalog.append_resources(
            server_id, [Node.from_dict(data) for data in node_data]
        )

    def run(self):
        """Run loop"""

        context = zmq.Context()
        socket = context.socket(zmq.DEALER)  # pylint: disable=no-member
        socket.setsockopt(zmq.IDENTITY, self.uid.encode("utf-8"))  # pylint: disable=no-member
        socket.set_hwm(50000)

        socket.connect("ipc://scheduler.ipc")
        self.transport = Transport(socket)
        self.schema = RequestSchema()
        self.transport.send_multipart(Message.notification("scheduler-alive"))

        self.log = get_storalloc_logger(self.verbose, True, self.uid)
        if self.remote_logging:
            add_remote_handler(
                self.log, self.uid, context, self.remote_logging[0], self.remote_logging[1]
            )
            self.strategy.set_logger(self.log)

        while True:

            identities, message = self.transport.recv_multipart()
            if message.category is MsgCat.REQUEST:
                request = self.schema.load(message.content)
                if request.state is ReqState.PENDING:
                    self.accumulate_request(request)
                elif request.state is ReqState.ENDED:
                    self.process_deallocation_request(request)
                else:
                    self.log.error(
                        f"Received undesired Request with state {request.state}"
                        + "Should have been either one of GRANTED or ENDED"
                    )
                    continue
            elif message.category is MsgCat.REGISTRATION:
                self.log.debug(f"Registration from server id : {identities}")
                self.process_node_registration(identities[0], message.content)
            elif message.category is MsgCat.NOTIFICATION:
                # Answer to keep alive messages from router
                notification = Message.notification("keep-alive")
                self.transport.send_multipart(notification)
