""" Storalloc
    Default router for the orchestrator
"""

import uuid
from multiprocessing import Process
import zmq

from storalloc.request import RequestSchema, ReqState

# TODO: Use the __init__ in strategies subpackage to import strategy object in a cleaner way
from storalloc.strategies.random_alloc import RandomAlloc
from storalloc.strategies.worst_case import WorstCase
from storalloc import resources
from storalloc.orchestrator.queue import AllocationQueue
from storalloc.orchestrator.scheduler import Scheduler
from storalloc.utils.config import config_from_yaml
from storalloc.utils.message import Message, MsgCat
from storalloc.utils.transport import Transport
from storalloc.utils.logging import get_storalloc_logger, add_remote_handler


# pylint: disable=logging-not-lazy,logging-fstring-interpolation


def make_strategy(strategy_name: str):
    """Create and return a scheduling strategy according to a given strategy name"""

    if strategy_name == "random_alloc":
        return RandomAlloc()
    if strategy_name == "worst_case":
        return WorstCase()

    raise ValueError(
        f"The scheduling strategy {strategy_name} specified in configuration does not exist"
    )


def make_resource_catalog(catalog_name: str):
    """Create and return a resource catalog"""

    if catalog_name == "inmemory":
        return resources.ResourceCatalog()

    raise ValueError(
        f"The resource catalog {catalog_name} specified in configuration does not exist"
    )


class Router:
    """Default router between components"""

    def __init__(self, config_path: str, uid: str = None, verbose: bool = True):
        """Init router"""

        self.uid = uid or f"R-{str(uuid.uuid4().hex)[:6]}"
        self.conf = config_from_yaml(config_path)
        self.log = get_storalloc_logger(verbose)
        # Init transports (as soon as possible after logger, as it will
        # possibly append a handler on it)
        self.transports = self.zmq_init()

        self.req_count = 0

        # Init scheduler
        self.scheduler = Scheduler(
            f"{self.uid}-SC",
            make_strategy(self.conf["sched_strategy"]),
            make_resource_catalog(self.conf["res_catalog"]),
            verbose,
            (
                f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_port']}",
                f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_sync_port']}",
            ),
        )

        # Init allocation queue manager
        self.queue_manager = AllocationQueue(
            f"{self.uid}-QM",
            verbose,
            (
                f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_port']}",
                f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_sync_port']}",
            ),
        )

        self.schema = RequestSchema()

    def zmq_init(self, remote_logging: bool = True):
        """Init ZMQ in order to be ready for connections"""

        context = zmq.Context()

        # Logging PUBLISHER and associated handler ######################################
        if remote_logging:
            add_remote_handler(
                self.log,
                self.uid,
                context,
                f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_port']}",
                f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_sync_port']}",
            )

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

        # Scheduler ROUTER ########################################################################
        self.log.info("Binding socket for scheduler process via IPC")
        scheduler_socket = context.socket(zmq.ROUTER)  # pylint: disable=no-member
        scheduler_socket.bind("ipc://scheduler.ipc")

        # Queue manager ROUTER ####################################################################
        self.log.info("Binding socket for queue manager process via IPC")
        queue_manager_socket = context.socket(zmq.ROUTER)  # pylint: disable=no-member
        queue_manager_socket.bind("ipc://queue_manager.ipc")

        # Simulation PUBLISHER ####################################################################
        self.log.info("Binding socket for publishing simulation updates")
        simulation_socket = context.socket(zmq.PUB)  # pylint: disable=no-member
        simulation_socket.bind(
            f"tcp://{self.conf['orchestrator_addr']}:{self.conf['simulation_port']}"
        )

        # Visualisation PUBLISHER #################################################################
        self.log.info("Binding socket for publishing visualisation updates")
        visualisation_socket = context.socket(zmq.PUB)  # pylint: disable=no-member
        visualisation_socket.bind(
            f"tcp://{self.conf['orchestrator_addr']}:{self.conf['o_visualisation_port']}"
        )

        # POLLER ##################################################################################
        self.log.info("Creating poller for client and server sockets")
        poller = zmq.Poller()
        poller.register(client_socket, zmq.POLLIN)
        poller.register(server_socket, zmq.POLLIN)
        # poller.register(sim_sync_socket, zmq.POLLIN)

        # IPC POLLER ##############################################################################
        self.log.info("Creating poller for scheduler and queue manager sockets")
        ipc_poller = zmq.Poller()
        ipc_poller.register(scheduler_socket, zmq.POLLIN)
        ipc_poller.register(queue_manager_socket, zmq.POLLIN)

        transports = {
            "client": Transport(client_socket),
            "server": Transport(server_socket),
            "scheduler": Transport(scheduler_socket),
            "queue": Transport(queue_manager_socket),
            "simulation": Transport(simulation_socket),
            "visualisation": Transport(visualisation_socket),
            "poller": poller,
            "ipc_poller": ipc_poller,
            "context": context,
        }

        return transports

    def process_client_message(self):
        """Process an incoming client message"""

        identities, message = self.transports["client"].recv_multipart()
        client_id = identities[0]

        if message.category == MsgCat.REQUEST:
            # Acknowledge request and send it on its way

            self.log.debug(f"Processing request from client {client_id}")

            req = self.schema.load(message.content)

            # Update request state
            req.job_id = f"{self.uid}-{self.req_count}"
            req.client_id = client_id
            req.state = ReqState.PENDING
            self.req_count += 1
            self.log.info(req)

            # To scheduler for processing
            pending_req = self.schema.dump(req)
            self.transports["scheduler"].send_multipart(
                Message(MsgCat.REQUEST, pending_req), f"{self.uid}-SC"
            )

            # To client for ack
            notification = Message.notification(
                f"Request PENDING and sent to scheduler, with ID {req.job_id}"
            )
            self.transports["client"].send_multipart(notification, client_id)

        elif message.category == MsgCat.EOS:
            self.log.debug("Processing EoS from client {client_id}")
            # Propagate the end of simulation flag to the simulation socket
            self.transports["simulation"].send_multipart(message, "sim")
        else:
            self.log.warning("Undesired message ({message.category}) received from a client")

    def process_server_message(self):
        """Process an incoming server message"""

        identities, message = self.transports["server"].recv_multipart()
        self.log.debug(f"Incoming message from server {identities}")

        if message.category == MsgCat.REGISTRATION:
            # Forward registration to the scheduler
            self.log.debug("Transmitting registration message to scheduler")
            self.transports["scheduler"].send_multipart(message, [f"{self.uid}-SC"] + identities)

            # Forward registration to the simulation and visualisation
            self.log.debug("Transmitting registration message to sim / visu")
            self.transports["simulation"].send_multipart(message, ["sim"] + identities)
            self.transports["visualisation"].send_multipart(message, "vis")

            self.log.debug("Done handling registration message")

        elif message.category == MsgCat.REQUEST:
            request = self.schema.load(message.content)
            if request.state == ReqState.ALLOCATED:
                # Relay request from server to client and ask queue manager to keep track of it
                self.log.debug(f"Transmitting request back to client {request.client_id}")
                self.transports["client"].send_multipart(message, request.client_id)
                self.transports["queue"].send_multipart(message)

                # Forward registration to the simulation and visualisation
                self.transports["simulation"].send_multipart(message, "sim")
                self.transports["visualisation"].send_multipart(message, "vis")
            elif request.state == ReqState.FAILED:
                error = Message.error(f"Requested allocation failed : {request.reason}")
                self.transports["client"].send_multipart(error, request.client_id)
        else:
            self.log.warning("Undesired message ({message.category}) received from a server")

    def process_scheduler_message(self):
        """Process incoming messages from scheduler. Those messages will always be REQUEST,
        either GRANTED or REFUSED"""

        identities, message = self.transports["scheduler"].recv_multipart()
        self.log.debug(f"Received message from scheduler {identities}")

        if message.category != MsgCat.REQUEST:
            self.log.warning(f"Undesired message ({message.category}) received from a scheduler")
            return

        request = self.schema.load(message.content)

        if request.state == ReqState.GRANTED:
            # Forward to server for actual allocation
            self.log.debug(
                f"Request [GRANTED], forwarding allocation request to server {request.server_id}"
            )
            self.transports["server"].send_multipart(message, request.server_id)
        elif request.state == ReqState.REFUSED:
            # Send an error to client
            error = Message.error(f"Requested allocation refused : {request.reason}")
            self.transports["client"].send_multipart(error, request.client_id)
        else:
            self.log.error(
                f"Request transmitted by scheduler has state {request.state},"
                + " and should be either one of GRANTED / REFUSED instead"
            )
            return

    def process_queue_message(self):
        """Process incoming messages from queue_manager. Those messages will always be
        REQUEST with the status ENDED, destined to the scheduler."""

        _, message = self.transports["queue"].recv_multipart()

        if message.category != MsgCat.REQUEST:
            self.log.warning("Undesired message ({message.category}) received from a queue manager")
            return

        request = self.schema.load(message)
        if request.state == ReqState.ENDED:
            # Notify server that he can reclaim the storage used by this request
            self.transports["server"].send_multipart(message)
            # Notify scheduler that the storage space used by this request will be available again
            self.transports["scheduler"].send_multipart(message)
        else:
            self.log.error(
                f"Request transmitted by queue manager has state {request.state},"
                + " and should only be ENDED instead"
            )
            return

    def start_process(self, process: Process, socket_name: str):
        """Start a process and check connectivity with it"""

        process.start()
        self.log.info(f"Started {socket_name} process with PID {process.pid}")

        # When the process is ready, we should receive a message
        if self.transports[socket_name].poll(timeout=3000):
            identities, message = self.transports[socket_name].recv_multipart()
            self.log.info(identities)
            self.log.info(message)
        else:
            raise ConnectionError(
                f"Unable to establish contact with IPC socket {socket_name} in sub process"
            )

    def run(self):
        """Start processes for scheduler and queue_manager, and run infinite loop
        for routing messages between every component. This is basically just a
        source-based routing process.

        All events from clients and servers are treated before all events from
        scheduler and queue_manager. Doing so, we're giving a chance to the scheduler
        and queue_manager to answer some of the client/server events right away, and not
        on the next loop iteration (TODO: testing required to confirm hypothesis)
        """

        # Start scheduler and queue manager process and ensure they're alive
        self.start_process(self.scheduler, "scheduler")
        self.start_process(self.queue_manager, "queue")

        while True:

            # Handle outside events from clients and servers
            events = dict(self.transports["poller"].poll(100))

            if events.get(self.transports["client"].socket) == zmq.POLLIN:
                self.log.debug("New client message")
                self.process_client_message()

            if events.get(self.transports["server"].socket) == zmq.POLLIN:
                self.log.debug("New server message")
                self.process_server_message()

            # Handle IPC events from scheduler and queue manager
            ipc_events = dict(self.transports["ipc_poller"].poll(100))

            if ipc_events.get(self.transports["scheduler"].socket) == zmq.POLLIN:
                self.log.debug("New scheduler message")
                self.process_scheduler_message()

            if ipc_events.get(self.transports["queue"].socket) == zmq.POLLIN:
                self.log.debug("New queue message")
                self.process_queue_message()
