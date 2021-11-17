""" Storalloc
    Default orchestrator
"""

import datetime
import logging
import uuid
import zmq

from storalloc.job import Job, JobStatus
from storalloc.request import RequestSchema, ReqState
from storalloc.sched_strategy import SchedStrategy
from storalloc import strategies
from storalloc.scheduler import Scheduler
from storalloc.resources import ResourceCatalog, Node
from storalloc.config import config_from_yaml
from storalloc.message import Message, MsgCat
from storalloc.logging import get_storalloc_logger


def recv_msg(socket):
    """Receive a message on a socket"""
    message_parts = socket.recv_multipart()
    identities, data = message_parts[:-1], message_parts[-1]
    return (identities, data)


def make_strategy(strategy_name: str):
    """Create and return a scheduling strategy according to a given strategy name"""

    if strategy_name == "random_alloc":
        return strategies.random_alloc.RandomAlloc()
    if strategy_name == "worst_case":
        return strategies.worst_case.WorstCase()

    raise ValueError(
        f"The scheduling strategy {strategy_name} specified in configuration does not exist"
    )


def make_resource_catalog(catalog_name: str):
    """Create and return a resource catalog"""

    if catalog_name == "inmemory":
        return ResourceCatalog()

    raise ValueError(
        f"The resource catalog {catalog_name} specified in configuration does not exist"
    )


class Orchestrator:
    """Default orchestrator"""

    def __init__(self, config_path: str):
        """Init orchestrator"""

        self.log = get_storalloc_logger()

        # Init transport (as soon as possible after logger, as it will
        # possibly append a handler on it)
        self.transport = self.zmq_init()

        self.app_id = str(uuid.uuid4())[:6]

        self.conf = config_from_yaml(config_path)

        self.req_count = 0

        # Init scheduler
        self.scheduler = Scheduler()
        self.scheduler.strategy = make_strategy(self.conf["sched_strategy"])
        self.scheduler.resource_catalog = make_resource_catalog(self.conf["res_catalog"])

        self.schema = RequestSchema()

    def zmq_init(self, remote_logging: bool = True):
        """Init ZMQ in order to be ready for connections"""

        context = zmq.Context()

        # Logging PUBLISHER and associated handler ######################################
        if remote_logging:
            log_publisher = context.socket(zmq.PUB)  # pylint: disable=no-member
            log_publisher.bind(f"{self.conf['orchestrator_addr']}:{self.conf['log_server_port']}")
            self.log.addHandler(zmq.PUBHandler(log_publisher))  # pylint: disable=no-member

        # Client and server ROUTERs #####################################################
        if self.conf["transport"] == "tcp":
            client_url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['client_port']}"
            server_url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['server_port']}"
        elif self.conf["transport"] == "ipc":
            client_url = f"ipc://{self.conf['orchestrator_fe_ipc']}.ipc"
            server_url = f"ipc://{self.conf['orchestrator_be_ipc']}.ipc"

        # Bind client socket
        self.log.info(f"Binding socket for client on {client_url}")
        client_socket = context.socket(zmq.ROUTER)  # pylint: disable=no-member
        client_socket.bind(client_url)

        # Bind server socket
        self.log.info(f"Binding socket for server on {server_url}")
        server_socket = context.socket(zmq.ROUTER)  # pylint: disable=no-member
        server_socket.bind(server_url)

        # Scheduler DEALER ########################################################################
        self.log.info(f"Connecting to scheduler process via IPC")
        scheduler_socket = context.socket(zmq.DEALER)  # pylint: disable=no-member
        scheduler_socket.connect("ipc://scheduler.ipc")

        # Queue manager DEALER ########################################################################
        self.log.info(f"Connecting to queue manager process via IPC")
        queue_manager_socket = context.socket(zmq.DEALER)  # pylint: disable=no-member
        queue_manager_socket.connect("ipc://queue_manager.ipc")

        # Simulation PUBLISHER ####################################################################
        self.log.info(f"Binding socket for publishing simulation updates")
        simulation_socket = context.socket(zmq.PUB)  # pylint: disable=no-member
        simulation_socket.bind(f"{self.conf['orchestrator_addr']}:{self.conf['simulation_port']}")

        # Visualisation PUBLISHER #################################################################
        self.log.info(f"Binding socket for publishing visualisation updates")
        visualisation_socket = context.socket(zmq.PUB)  # pylint: disable=no-member
        visualisation_socket.bind(
            f"{self.conf['orchestrator_addr']}:{self.conf['visualisation_port']}"
        )

        # POLLER ##################################################################################
        self.log.info("Creating poller for client, server and scheduler sockets")
        poller = zmq.Poller()
        poller.register(server_socket, zmq.POLLIN)
        poller.register(client_socket, zmq.POLLIN)
        poller.register(scheduler_socket, zmq.POLLIN)

        transport = {
            "client": client_socket,
            "server": server_socket,
            "scheduler": scheduler_socket,
            "queue": queue_manager_socket,
            "simulation": simulation_socket,
            "visualisation": visualisation_socket,
            "poller": poller,
            "context": context,
        }

        return transport

    def grant_allocation(self, job: Job, target_node: int, target_disk: int):
        """Grant a storage request and register it"""

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
        # notification.send(self.client_socket, job.client_identity)

    def process_client_message(self):
        """Process an incoming client message"""

        identities, data = recv_msg(self.client_socket)
        message = Message.unpack(data)
        client_id = identities[0]

        if message.category == MsgCat.REQUEST:
            try:
                req = self.schema.load(message.content)
                # req = Request(message.content)
            except ValueError as exc:
                error = Message.error(f"Wrong request {exc}")
                self.client_socket.send_multipart([client_id, error])
                # TODO : raise ?
                return

            req.job_id = f"{self.app_id}-{self.req_count}"
            req.client_id = client_id
            req.state = ReqState.PENDING
            self.req_count += 1

            notification = Message.notification(f"Pending request allocation {req.job_id}")
            self.client_socket.send_multipart([client_id, notification])

            # notification.send(self.client_socket, client_id)
        elif message.category == MsgCat.EOS:
            end_of_simulation = True
            notification = Message(MsgCat.SHUTDOWN, None)
            self.client_socket.send_multipart([client_id, notification.pack()])
            # notification.send(self.client_socket, client_id)
        else:
            self.log.warning("Unknown message cat. ({message.category}) received from a client")

    def process_server_message(self):
        """Process an incoming server message"""

        identities, data = recv_msg(self.server_socket)
        message = Message.unpack(data)

        # Process storage server registration internally
        if message.category == MsgCat.REGISTRATION:
            server_id = identities[0]
            self.rcatalog.append_resources(
                server_id, [Node.from_dict(data) for data in message.content]
            )
            logging.debug("Server registered. New resources available.")
            # TODO: setup monitoring system with newly added resources

        # Relay server -> client messages
        elif message.category == MsgCat.REQUEST:
            client_id = identities[1]
            notification = Message(MsgCat.REQUEST, message.content)
            self.client_socket.send_multipart([client_id, notification.pack()])
            # notification.send(self.client_socket, client_id)
        else:
            self.log.warning("Unknown message cat. ({message.category}) received from a server")

    def process_queue(self, simulate: bool):
        """Process request queue"""

        if simulate:
            if end_of_simulation:

                earliest_start_time = datetime.datetime.now()
                latest_end_time = datetime.datetime(1970, 1, 1)

                self.pending_jobs.sort_asc_start_time()

                for job in self.pending_jobs:
                    if job.start_time() < earliest_start_time:
                        earliest_start_time = job.start_time()
                    if job.end_time() > latest_end_time:
                        latest_end_time = job.end_time()

                sim_duration = (latest_end_time - earliest_start_time).total_seconds() + 1

                for job in self.pending_jobs:
                    self.env.process(self.simulate_scheduling)

                self.env.run(until=sim_duration)
        else:
            for job in self.pending_jobs:
                target_node, target_disk = self.scheduling_strategy.compute(self.rcatalog, job)

                # If a disk on a node has been found, we allocate the request
                if target_node >= 0 and target_disk >= 0:
                    self.grant_allocation(job, target_node, target_disk)
                else:
                    if not job.is_pending():
                        self.log.debug(
                            f"Job<{job.uid:05}> - Currently unable to allocate incoming request"
                        )
                        job.status = JobStatus.PENDING

    def run(self, simulate: bool):
        """Start an infinite loop and process incoming / outgoing messages"""

        while True:

            events = dict(self.poller.poll(100))

            ## CLIENT SOCKET
            if events.get(self.client_socket) == zmq.POLLIN:
                self.process_client_message()

            ## SERVER SOCKET
            if events.get(self.server_socket) == zmq.POLLIN:
                self.process_server_message()

            ## SCHEDULER SOCKET
            if events.get(self.scheduler_socket) == zmq.POLLIN:
                self.process_scheduler_message()

            ## PROCESS QUEUES
            self.process_queue(simulate)
