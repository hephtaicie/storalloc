""" Storalloc
    Default router for the orchestrator
"""

import uuid
from multiprocessing import Process
import zmq

from storalloc.request import RequestSchema, ReqState
from storalloc import strategies
from storalloc import resources
from storalloc.orchestrator.queue import AllocationQueue
from storalloc.orchestrator.scheduler import Scheduler
from storalloc.utils.config import config_from_yaml
from storalloc.utils.message import Message, MsgCat
from storalloc.utils.logging import get_storalloc_logger


# pylint: disable=logging-not-lazy,logging-fstring-interpolation


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
        return resources.ResourceCatalog()

    raise ValueError(
        f"The resource catalog {catalog_name} specified in configuration does not exist"
    )


class Router:
    """Default router between components"""

    def __init__(self, config_path: str, verbose: bool = True):
        """Init router"""

        self.conf = config_from_yaml(config_path)
        self.log = get_storalloc_logger(verbose)
        # Init transports (as soon as possible after logger, as it will
        # possibly append a handler on it)
        self.transports = self.zmq_init()

        self.app_id = str(uuid.uuid4())[:6]
        self.req_count = 0

        # Init scheduler
        self.scheduler = Scheduler()
        self.scheduler.strategy = make_strategy(self.conf["sched_strategy"])
        self.scheduler.resource_catalog = make_resource_catalog(self.conf["res_catalog"])

        # Init allocation queue manager
        self.queue_manager = AllocationQueue()

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
        if self.conf["transports"] == "tcp":
            client_url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['client_port']}"
            server_url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['server_port']}"
        elif self.conf["transports"] == "ipc":
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
        self.log.info("Connecting to scheduler process via IPC")
        scheduler_socket = context.socket(zmq.DEALER)  # pylint: disable=no-member
        scheduler_socket.connect("ipc://scheduler.ipc")

        # Queue manager DEALER ####################################################################
        self.log.info("Connecting to queue manager process via IPC")
        queue_manager_socket = context.socket(zmq.DEALER)  # pylint: disable=no-member
        queue_manager_socket.connect("ipc://queue_manager.ipc")

        # Simulation PUBLISHER ####################################################################
        self.log.info("Binding socket for publishing simulation updates")
        simulation_socket = context.socket(zmq.PUB)  # pylint: disable=no-member
        simulation_socket.bind(f"{self.conf['orchestrator_addr']}:{self.conf['simulation_port']}")

        # Visualisation PUBLISHER #################################################################
        self.log.info("Binding socket for publishing visualisation updates")
        visualisation_socket = context.socket(zmq.PUB)  # pylint: disable=no-member
        visualisation_socket.bind(
            f"{self.conf['orchestrator_addr']}:{self.conf['visualisation_port']}"
        )

        # POLLER ##################################################################################
        self.log.info("Creating poller for client and server sockets")
        poller = zmq.Poller()
        poller.register(server_socket, zmq.POLLIN)
        poller.register(client_socket, zmq.POLLIN)

        # IPC POLLER ##############################################################################
        self.log.info("Creating poller for scheduler and queue manager sockets")
        ipc_poller = zmq.Poller()
        ipc_poller.register(scheduler_socket, zmq.POLLIN)
        ipc_poller.register(queue_manager_socket, zmq.POLLIN)

        transports = {
            "client": client_socket,
            "server": server_socket,
            "scheduler": scheduler_socket,
            "queue": queue_manager_socket,
            "simulation": simulation_socket,
            "visualisation": visualisation_socket,
            "poller": poller,
            "ipc_poller": ipc_poller,
            "context": context,
        }

        return transports

    def process_client_message(self):
        """Process an incoming client message"""

        identities, data = recv_msg(self.transports["client"])
        message = Message.unpack(data)
        client_id = identities[0]

        if message.category == MsgCat.REQUEST:
            # Acknowledge request and send it on its way

            req = self.schema.load(message.content)

            req.job_id = f"{self.app_id}-{self.req_count}"
            req.client_id = client_id
            req.state = ReqState.PENDING
            self.req_count += 1

            # To scheduler for processing
            pending_req = self.schema.dump(req)
            self.transports["scheduler"].send_multipart(
                [client_id, Message(MsgCat.REQUEST, pending_req).pack()]
            )

            # To client for ack
            notification = Message.notification(
                f"Request PENDING and sent to scheduler, with ID {req.job_id}"
            )
            self.transports["client"].send_multipart([client_id, notification])

        elif message.category == MsgCat.EOS:
            # Propagate the end of simulation flag to the simulation socket
            self.transports["simulation"].send_multipart(identities + [data])
        else:
            self.log.warning("Undesired message ({message.category}) received from a client")

    def process_server_message(self):
        """Process an incoming server message"""

        identities, data = recv_msg(self.transports["server"])
        message = Message.unpack(data)

        if message.category == MsgCat.REGISTRATION:
            # Forward registration to the scheduler
            self.log.debug("Transmitting registration message to scheduler")
            self.transports["scheduler"].send_multipart(identities + [data])

            # Forward registration to the simulation and visualisation
            self.transports["simulation"].send_multipart(identities + [data])
            self.transports["visualise"].send_multipart(identities + [data])

        elif message.category == MsgCat.REQUEST:
            request = self.schema.load(message)
            if request.state == ReqState.ALLOCATED:
                # Relay request from server to client and ask queue manager to keep track of it
                self.log.debug("Transmitting request back to client {identities[1]}")
                client_id = identities[1]
                self.transports["client"].send_multipart([client_id, data])
                self.transports["queue"].send_multipart([client_id, data])

                # Forward registration to the simulation and visualisation
                self.transports["simulation"].send_multipart([client_id, data])
                self.transports["visualise"].send_multipart([client_id, data])
            elif request.state == ReqState.FAILED:
                error = Message.error(f"Requested allocation failed : {request.reason}")
                self.transports["client"].send_multipart([client_id, error])
        else:
            self.log.warning("Undesired message ({message.category}) received from a server")

    def process_scheduler_message(self):
        """Process incoming messages from scheduler. Those messages will always be REQUEST,
        either GRANTED or REFUSED"""

        identities, data = recv_msg(self.transports["scheduler"])
        message = Message.unpack(data)

        if message.category != MsgCat.REQUEST:
            self.log.warning("Undesired message ({message.category}) received from a scheduler")
            return

        request = self.schema.load(message)

        if request.state == ReqState.GRANTED:
            # Forward to server for actual allocation
            self.transports["server"].send_multipart(identities + [data])
        elif request.state == ReqState.REFUSED:
            # Send an error to client
            error = Message.error(f"Requested allocation refused : {request.reason}")
            self.transports["client"].send_multipart([identities[1], error])
        else:
            self.log.error(
                f"Request transmitted by scheduler has state {request.state},"
                + " and should be either one of GRANTED / REFUSED instead"
            )
            return

    def process_queue_message(self):
        """Process incoming messages from queue_manager. Those messages will always be
        REQUEST with the status ENDED, destined to the scheduler."""

        identities, data = recv_msg(self.transports["queue"])
        message = Message.unpack(data)

        if message.category != MsgCat.REQUEST:
            self.log.warning("Undesired message ({message.category}) received from a queue manager")
            return

        request = self.schema.load(message)
        if request.state == ReqState.ENDED:
            # Notify server that he can reclaim the storage used by this request
            self.transports["server"].send_multipart(identities + [data])
            # Notify scheduler that the storage space used by this request will be available again
            self.transports["scheduler"].send_multipart(identities + [data])
        else:
            self.log.error(
                f"Request transmitted by queue manager has state {request.state},"
                + " and should only be ENDED instead"
            )
            return

    def start_process(self, process_run: any, socket_name: str):
        """Start a process and check connectivity with it"""

        process = Process(target=process_run)
        process.start()
        self.log.info(f"Started {socket_name} process with PID {process.pid}")

        self.transports[socket_name].send_multipart([Message.notification("keep_alive")])
        if self.transports[socket_name].poll(timeout=1000):
            self.transports[socket_name].recv_multipart()
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
        self.start_process(self.scheduler.run, "scheduler")
        self.start_process(self.queue_manager.run, "queue")

        while True:

            # Handle outside events from clients and servers
            events = dict(self.transports["poller"].poll(100))

            if events.get(self.transports["client"]) == zmq.POLLIN:
                self.process_client_message()

            if events.get(self.transports["server"]) == zmq.POLLIN:
                self.process_server_message()

            # Handle IPC events from scheduler and queue manager
            ipc_events = dict(self.transports["ipc_poller"].poll(100))

            if ipc_events.get(self.transports["scheduler"]) == zmq.POLLIN:
                self.process_scheduler_message()

            if ipc_events.get(self.transports["queue"]) == zmq.POLLIN:
                self.process_queue_message()
