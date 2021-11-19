""" Scheduler class

"""

from multiprocessing import Process

import zmq

from storalloc.strategies.base import StrategyInterface
from storalloc.resources import ResourceCatalog
from storalloc.utils.transport import Transport
from storalloc.utils.message import Message, MsgCat
from storalloc.request import RequestSchema, ReqState


class Scheduler(Process):
    """Scheduler class should run in its own process. It maintain an updated resource catalog
    made from the aggregation of 1..N resource catalog registered by storage server(s) and it
    runs a scheduling algorithm other this resource catalog in order to determine an optimal
    storage allocation placement for each request it receives.
    """

    def __init__(
        self, uid: str, strategy: StrategyInterface = None, resource_catalog: ResourceCatalog = None
    ):
        """Init process"""
        super().__init__()
        self.uid = uid
        self.strategy = strategy
        self.resource_catalog = resource_catalog

    def process_allocation_request(self, request):
        """Run scheduling algo and attempt to find an optimal allocation for a given request"""

        self.log.debug(
            f"[{job.uid:05}] Add {job.request} on node {target_node}, disk {target_disk}"
        )

        alloc_request = {
            "job_id": job.uid,
            "disk": target_disk,
            "capacity": job.request.capacity,
            "duration": job.request.duration,
        }
        identities = [
            self.rcatalog.identity_of_node(target_node),
            job.client_identity,
        ]
        notification = Message(MsgCat.ALLOCATION, alloc_request)
        self.server_socket.send_multipart(identities + [notification.pack()])
        # notification.send(self.server_socket, identities)

        job.status = JobStatus.ALLOCATED
        self.running_jobs.add(job)
        self.pending_jobs.remove(job)

        self.rcatalog.add_allocation(target_node, target_disk, job)
        self.rcatalog.print_status(target_node, target_disk)

        notification = Message(MsgCat.NOTIFICATION, f"Granted job allocation {job.uid}")
        self.client_socket.send_multipart([job.client_identity, notification.pack()])
        # notification.send(self.client_socket, job.client_identit

    def process_deallocation_request(self, request):
        """Acknowledge the release of some storage resource in the resource catalog
        (request in ENDED state)"""

    def process_node_registration(self, node_id: str, node_data: dict):
        """Update resource catalog according to a new registration"""
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

        while True:

            identities, message = self.transport.recv_multipart()
            print("Message received in scheduler")
            if message.category is MsgCat.REQUEST:
                print("Message is a request")
                request = self.schema.load(message.content)
                if request.state is ReqState.PENDING:
                    print("Message is a pending request")
                    self.process_allocation_request(request)
                elif request.state is ReqState.ENDED:
                    print("Message is an ended request")
                    self.process_deallocation_request(request)
                else:
                    # self.log.error(
                    #    f"Received undesired Request with state {request.state}"
                    #    + "Should have been either one of GRANTED or ENDED"
                    # )
                    continue
            elif message.category is MsgCat.REGISTRATION:
                print("Message is a registration")
                self.process_node_registration(identities[0], message.content)
            elif message.category is MsgCat.NOTIFICATION:
                print("Scheduler process received a notification")
                # Answer to keep alive messages from router
                notification = Message.notification("keep-alive")
                self.transport.send_multipart(notification)
