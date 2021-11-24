""" Storalloc Simulation module
    Handler for simulation event and Simpy wrapper.
"""

import uuid
import simpy
import zmq

from storalloc.request import RequestSchema, ReqState, StorageRequest
from storalloc.resources import ResourceCatalog, Node
from storalloc.utils.message import MsgCat
from storalloc.utils.logging import get_storalloc_logger, add_remote_handler
from storalloc.utils.config import config_from_yaml
from storalloc.utils.transport import Transport


# pylint: disable=no-member


class Simulator:
    """basic simulator"""

    def __init__(self, config_path: str, uid: str = None, verbose: bool = True):
        """Init simulation with new simpy env"""

        self.uid = uid or f"SIM-{str(uuid.uuid4().hex)[:6]}"
        self.verbose = verbose
        self.conf = config_from_yaml(config_path)
        self.log = get_storalloc_logger(verbose)

        self.schema = RequestSchema()

        self.resource_catalog = ResourceCatalog()

        self.transports = self.zmq_init()
        self.env = simpy.Environment()

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

        # Simulation SUBSCRIBER ####################################################################
        self.log.info("Connecting socket for subscribing to simulation updates")
        simulation_socket = context.socket(zmq.SUB)  # pylint: disable=no-member
        simulation_socket.setsockopt(zmq.SUBSCRIBE, b"sim")
        simulation_socket.connect(
            f"tcp://{self.conf['simulation_addr']}:{self.conf['simulation_port']}"
        )

        # Synchronisation ROUTER
        self.log.info("Binding socket for simulation sync")
        sync_signal = context.socket(zmq.ROUTER)
        sync_signal.bind(
            f"tcp://{self.conf['simulation_addr']}:{self.conf['simulation_sync_port']}"
        )

        # POLLER ##################################################################################
        self.log.info("Creating poller for simulation and sync sockets")
        poller = zmq.Poller()
        poller.register(simulation_socket, zmq.POLLIN)
        poller.register(sync_signal, zmq.POLLIN)

        transports = {
            "simulation": Transport(simulation_socket),
            "sync": Transport(sync_signal),
            "poller": poller,
            "context": context,
        }

        return transports

    """
    def simulate_scheduling(self, job, earliest_start_time):

        yield self.env.timeout(job.sim_start_time(earliest_start_time))

        target_node, target_disk = self.scheduling_strategy.compute(self.rcatalog, job)

        # If a disk on a node has been found, we allocate the request
        if target_node >= 0 and target_disk >= 0:
            self.grant_allocation(job, target_node, target_disk)
        else:
            self.log.warning(f"Job<{job.uid:05}> - Unable to allocate request. Exiting...")

        # Duration + Fix seconds VS minutes
        yield self.env.timeout(job.sim_start_time())

    def process_queue(self, simulate: bool):

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
    """

    def update_disk(self, server_id, node_id, disk_id, allocation):
        """Update used capacity for disk"""

        node = self.resource_catalog.get_node(server_id, node_id)
        container = node.disks[disk_id].sim_container
        # reserve some disk space
        if allocation > 0:
            if container.level + allocation > container.capacity:
                self.log.warning(
                    f"Allocation on {server_id}:{node_id}:{disk_id}"
                    + " will have to be postponned as capacity is not sufficient"
                )
            container.put(allocation)
        # free some disk space
        else:
            if container.level - allocation < 0:
                self.log.error(
                    f"Deallocation on {server_id}:{node_id}:{disk_id}"
                    + "causes level to get below 0. Something bad is happening"
                )
            container.get(-allocation)

    def allocate(self, request: StorageRequest):
        """Simulate allocation"""
        self.log.debug(
            f"Allocating {request.capacity}GB for {request.client_id} on "
            + f"{request.server_id}:{request.node_id}:{request.disk_id}"
        )
        self.log.debug(f"Duration is {request.duration} / start_time is {request.start_time}")
        storage = yield simpy.events.Timeout(
            self.env, delay=int(request.start_time.timestamp()), value=request.capacity
        )
        self.update_disk(request.server_id, request.node_id, request.disk_id, storage)
        self.log.debug(f"Request from {request.client_id} allocated to disk")

        yield simpy.events.Timeout(self.env, delay=request.duration.seconds)
        self.update_disk(request.server_id, request.node_id, request.disk_id, -storage)
        self.log.debug(f"Request from {request.client_id} deallocated from disk")

    def process_request(self, request: StorageRequest):
        """Process a storage request, which might be any state"""

        self.log.debug("Processing new storage request")

        if request.state is ReqState.OPENED:
            self.log.debug("Request is in OPENED state")
        elif request.state is ReqState.PENDING:
            self.log.debug("Request is in PENDING state")
        elif request.state is ReqState.GRANTED:
            self.log.debug("Request is in GRANTED state")
        elif request.state is ReqState.REFUSED:
            self.log.debug("Request is in REFUSED state")
        elif request.state is ReqState.ALLOCATED:
            self.log.debug("Request is in ALLOCATED state")
            self.env.process(self.allocate(request))
        elif request.state is ReqState.FAILED:
            self.log.debug("Request is in FAILED state")
        elif request.state is ReqState.ENDED:
            self.log.debug("Request is in ENDED state")
        else:
            self.log.warning("Request is in an unknown state")

    def process_registration(self, identity: bytes, node_data: dict):
        """Process new registration from server (by adding containers in the simulation)"""

        self.log.debug(f"Registration from server id : {identity}")

        for data in node_data:
            node = Node.from_dict(data)
            self.log.info(f"Processing new node registration for node {node.hostname}")

            for disk in node.disks:
                # Colocate a simulation container with each disk of a node.
                self.log.info(
                    f"New simpy.Container for disk {disk.uid}, with capacity {disk.capacity}"
                )
                setattr(
                    disk, "sim_container", simpy.Container(self.env, init=0, capacity=disk.capacity)
                )

            self.resource_catalog.append_resources(identity, [node])

    def simulation(self):
        """Run the simulation"""
        self.env.run()

    def run(self):
        """Infinite loop for receiving events"""

        while True:
            try:

                events = dict(self.transports["poller"].poll())
                if events.get(self.transports["simulation"].socket) == zmq.POLLIN:
                    identity, message = self.transports["simulation"].recv_multipart()
                    if message.category is MsgCat.REQUEST:
                        self.process_request(self.schema.load(message.content))
                    elif message.category is MsgCat.REGISTRATION:
                        self.process_registration(identity[1], message.content)
                    else:
                        self.log(
                            "Undesired message category received "
                            + f"({message.category}, silently discarding"
                        )

                if events.get(self.transports["sync"].socket) == zmq.POLLIN:
                    identity = self.transports["sync"].recv_sync_router()
                    self.transports["sync"].send_sync(identity)

            except zmq.ZMQError as err:
                if err.errno == zmq.ETERM:
                    break
                raise
            except KeyboardInterrupt:
                print("[!] Simulation server stopped by Ctrl-C - Now running simulation")
                self.simulation()
                break

        self.transports["context"].destroy()
