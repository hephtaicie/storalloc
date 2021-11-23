""" Scheduler class

"""

from multiprocessing import Process

import zmq

from storalloc.utils.logging import get_storalloc_logger, add_remote_handler
from storalloc.strategies.base import StrategyInterface
from storalloc.resources import ResourceCatalog, Node
from storalloc.utils.transport import Transport
from storalloc.utils.message import Message, MsgCat
from storalloc.request import RequestSchema, ReqState, StorageRequest


# pylint: disable=logging-not-lazy


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
        resource_catalog: ResourceCatalog = None,
        verbose: bool = False,
        remote_logging: tuple = None,
    ):
        """Init process"""
        super().__init__()
        self.uid = uid
        self.strategy = strategy
        self.resource_catalog = resource_catalog
        self.schema = RequestSchema()
        self.transport = None  # waiting for context init to run when process is started
        self.log = None  # same
        self.verbose = verbose
        self.remote_logging = remote_logging

    def process_allocation_request(self, request: StorageRequest):
        """Run scheduling algo and attempt to find an optimal allocation for a given request"""

        self.log.debug(f"Processing PENDING allocation request : {request}")

        target_node, target_disk = self.strategy.compute(self.resource_catalog, request)

        if target_node != -1:
            request.node_id = target_node
            request.disk_id = target_disk
            request.server_id = self.resource_catalog.get_node(target_node).identity
            request.state = ReqState.GRANTED
            self.log.debug(
                f"Request [GRANTED] on disk {target_node}:{target_disk} "
                + f"from server {request.server_id}"
            )
            self.resource_catalog.add_allocation(target_node, target_disk, request)
        else:
            request.state = ReqState.REFUSED
            request.reason = "Could not fit request onto current resources"

        # Send back the allocated request to the orchestrator
        self.transport.send_multipart(Message(MsgCat.REQUEST, self.schema.dump(request)))

    def process_deallocation_request(self, request):
        """Acknowledge the release of some storage resource in the resource catalog
        (request in ENDED state)"""
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

        socket.connect("ipc://scheduler.ipc")
        self.transport = Transport(socket)
        self.schema = RequestSchema()
        self.transport.send_multipart(Message.notification("scheduler-alive"))

        self.log = get_storalloc_logger(self.verbose, True, self.uid)
        if self.remote_logging:
            add_remote_handler(
                self.log, self.uid, context, self.remote_logging[0], self.remote_logging[1]
            )

        while True:

            identities, message = self.transport.recv_multipart()
            if message.category is MsgCat.REQUEST:
                request = self.schema.load(message.content)
                if request.state is ReqState.PENDING:
                    self.process_allocation_request(request)
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
