""" Storalloc Simulation module
    Handler for simulation event and Simpy wrapper.
"""
from multiprocessing import Process
from time import sleep
import uuid
import zmq
import simpy
import yaml

from storalloc.request import RequestSchema, ReqState, StorageRequest
from storalloc.resources import ResourceCatalog, Node
from storalloc.visualisation import Visualisation
from storalloc.utils.message import Message, MsgCat
from storalloc.utils.logging import get_storalloc_logger, add_remote_handler
from storalloc.utils.config import config_from_yaml
from storalloc.utils.transport import Transport


# pylint: disable=no-member,logging-not-lazy,too-many-instance-attributes,logging-fstring-interpolation


def start_visualisation(path: str):
    """Start a new process with a visualisation server"""
    v_uid = f"VIS-{str(uuid.uuid4().hex)[:6]}"
    visu = Visualisation(path, v_uid)
    visu.run()


def summarise_ressource_catalog(resource_catalog: ResourceCatalog):
    """Compile a few stats about current state of the resource catalog"""

    res = {
        "total_allocated": 0,
        "max_concurrent_alloc_disk": 0,
        "max_concurrent_alloc_node": 0,
    }

    for _, nodes in resource_catalog.storage_resources.items():
        for node in nodes:
            res["max_concurrent_alloc_node"] = max(
                res["max_concurrent_alloc_node"], node.sim_nb_alloc
            )
            for disk in node.disks:
                res["total_allocated"] += disk.sim_container.level
                res["max_concurrent_alloc_disk"] = max(
                    res["max_concurrent_alloc_disk"], disk.sim_nb_alloc
                )

    return res


# pylint: disable=too-many-arguments
class Simulation:
    """Basic simulator. Outputs a yaml file with a summary of the
    simulation and send regular updates to any visualisation or log server
    up before the simulation starts.
    """

    def __init__(
        self,
        config_path: str,
        uid: str = None,
        verbose: bool = False,
        rt_factor: float = 1.0,
        visualisation: bool = True,
        remote_logging: bool = False,
    ):
        """Init simulation with new simpy env"""

        self.uid = uid or f"SIM-{str(uuid.uuid4().hex)[:6]}"
        self.config_path = config_path
        self.conf = config_from_yaml(config_path)
        self.log = get_storalloc_logger(verbose, remote_logging, "sim-server")

        self.schema = RequestSchema()

        self.resource_catalog = ResourceCatalog()

        self.transports = self.zmq_init(remote_logging)
        if rt_factor == 1.0:
            self.env = simpy.Environment()
        else:
            self.env = simpy.rt.RealtimeEnvironement(factor=rt_factor)

        self.visualisation = visualisation

        ## Everything related to simulation statistics ##
        self.split_request_ids = {}
        self.split_request_blacklist = set()
        self.stats = {
            "concurrent_allocations": 0,  # current number of concurrent allocations
            "max_ca": 0,  # max concurrent allocations until now
            "requests_nb": 0,  # Total number of requests received (any state)
            "registrations_nb": 0,  # Number of server registrations reveived
            "scheduler_fallbacks": 0,  # Requests received by simulation in REFUSED state
            "simulation_fallbacks": 0,  # Requests proved to be invalid when unrolling simulation
            "total_waiting_time_minutes": 0,  # Total cumulated waiting time for delayed requests
            "delayed_requests": 0,  # Number of requests that have been delayed (GRANTED state only)
            "allocated_delayed_requests": 0,  # Nb of delayed and successfully allocated reqs
            "refused_delayed_requests": 0,  # Number of requests delayed and still refused by sched
            "split_requests": 0,  # Tt nb of split requests (even those bound to fail)
            "allocated_split_requests": 0,  # Nb of split reqs whose subreqs have all been allocated
            "failed_split_requests": 0,  # Nb of split reqs with at least one failure in sub reqs
            "refused_split_requests": 0,  # Nb of split reqs refused by scheduler before simulation
            "total_gb_alloc": 0,
            "total_gb_dealloc": 0,
            "first_event_time": 0,  # First allocation
            "last_event_time": 0,  # Last deallocation
        }

        self.log.info(f"Simulation server {self.uid} initiated.")

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
        simulation_socket.setsockopt(zmq.SUBSCRIBE, b"sim")  # pylint: disable=no-member
        simulation_socket.setsockopt(zmq.LINGER, 0)
        simulation_socket.setsockopt(zmq.IMMEDIATE, 1)
        simulation_socket.connect(
            f"tcp://{self.conf['orchestrator_addr']}:{self.conf['simulation_port']}"
        )

        # Visualisation PUBLISHER #################################################################
        self.log.info("Binding socket for publishing visualisation updates")
        visualisation_socket = context.socket(zmq.PUB)  # pylint: disable=no-member
        visualisation_socket.set_hwm(50000)
        visualisation_socket.setsockopt(zmq.LINGER, 0)
        simulation_socket.setsockopt(zmq.IMMEDIATE, 1)
        visualisation_socket.bind(
            f"tcp://{self.conf['simulation_addr']}:{self.conf['s_visualisation_port']}"
        )

        # POLLER ##################################################################################
        self.log.info("Creating poller for simulation and sync sockets")
        poller = zmq.Poller()
        poller.register(simulation_socket, zmq.POLLIN)
        #       poller.register(sync_signal, zmq.POLLIN)

        transports = {
            "simulation": Transport(simulation_socket),
            "visualisation": Transport(visualisation_socket),
            "poller": poller,
            "context": context,
        }

        return transports

    def track_split_request(self, request: StorageRequest):
        """Split request accounting (for not yet allocated requests or for
        allocated requests, depending on the split_store)"""

        # Ignore non-split requests
        if request.divided == 1:
            return

        # Extract split number (which can be 0, in which case we don't do
        # anything)
        full_job_id = request.job_id
        last_dash = full_job_id.rfind("-")
        short_job_id = full_job_id[:last_dash]

        if short_job_id not in self.split_request_ids:
            self.stats["split_requests"] += 1
            self.split_request_ids[short_job_id] = {}

        self.split_request_ids[short_job_id][full_job_id] = {
            "sid": request.server_id,
            "nid": request.node_id,
            "did": request.disk_id,
            "storage": request.capacity,
            "allocated": False,
        }

    def split_blacklisted(self, request):
        """Check whether a split is from a blacklisted request or not"""

        # Ignore non-split requests
        if request.divided == 1:
            return False

        full_job_id = request.job_id
        last_dash = full_job_id.rfind("-")
        short_job_id = full_job_id[:last_dash]
        if short_job_id in self.split_request_blacklist:
            return True

        return False

    def invalidate_split(self, request):
        """Invalidate a split request because one of the sub-requests has failed to be allocated"""

        # Ignore non-split requests
        if request.divided == 1:
            return

        self.log.warning(f"Initiating mass deallocation process for {request.job_id}")

        full_job_id = request.job_id
        last_dash = full_job_id.rfind("-")
        short_job_id = full_job_id[:last_dash]
        self.split_request_blacklist.add(short_job_id)

        self.stats["allocated_split_requests"] -= 1
        self.stats["failed_split_requests"] += 1

        # Deallocate every subrequest that was already processed
        dealloc_nb = 0
        for sub_req in self.split_request_ids[short_job_id].values():
            if sub_req["allocated"] is True:
                self.update_disk(
                    sub_req["sid"],
                    sub_req["nid"],
                    sub_req["did"],
                    -sub_req["storage"],
                )
                dealloc_nb += 1

        # Remove split entry from split request catalog
        del self.split_request_ids[short_job_id]
        self.log.warning(f"Deallocated {dealloc_nb} sub reqs for {request.job_id}")

    def validate_split(self, request):
        """Acknowledge that a sub request in a split request has correctly been allocated"""

        # Ignore non-split requests
        if request.divided == 1:
            return

        full_job_id = request.job_id
        last_dash = full_job_id.rfind("-")
        short_job_id = full_job_id[:last_dash]
        if full_job_id[last_dash:] == "-0":
            self.stats["allocated_split_requests"] += 1
        self.split_request_ids[short_job_id][full_job_id]["allocated"] = True

    def update_disk(self, server_id, node_id, disk_id, allocation):
        """Update used capacity for disk"""

        node = self.resource_catalog.get_node(server_id, node_id)
        disk = node.disks[disk_id]
        container = disk.sim_container

        #### Try to allocate the requested capacity on virtual storage
        if allocation > 0:

            if container.level + allocation > container.capacity:
                self.log.warning(
                    f"[SIM ERROR] Allocation on {server_id}:{node_id}:{disk_id}"
                    + " can't happen as capacity is not sufficient"
                )
                self.stats["simulation_fallbacks"] += 1
                raise ValueError("Not enough space (allocation)")

            # DISK/NODE-related stats: Mean capacity utilisation and mean number of allocations
            if disk.sim_last_alloc_time:
                disk.sim_mean_nb_alloc += (
                    self.env.now - disk.sim_last_alloc_time
                ) * disk.sim_nb_alloc
                disk.sim_mean_cap_utilisation += (self.env.now - disk.sim_last_alloc_time) * (
                    ((container.level) * 100) / container.capacity
                )

            if node.sim_last_alloc_time:
                node.sim_mean_nb_alloc += (
                    self.env.now - node.sim_last_alloc_time
                ) * node.sim_nb_alloc

            ### --> Storage allocation happens here
            container.put(allocation)
            ### <-- ###

            # Update max capacity utilisation / max number of allocations of current disk
            utilisation = (container.level * 100) / container.capacity
            if utilisation > disk.sim_max_cap_utilisation:  # max utilisation per disk
                disk.sim_max_cap_utilisation = utilisation
            disk.sim_nb_alloc += 1
            if disk.sim_nb_alloc > disk.sim_max_alloc:  # max alloc per disk
                disk.sim_max_alloc = disk.sim_nb_alloc

            node.sim_nb_alloc += 1
            disk.sim_last_alloc_time = self.env.now
            node.sim_last_alloc_time = self.env.now

            # Global stats and real-time visualisation data #
            self.stats["concurrent_allocations"] += 1
            self.stats["total_gb_alloc"] += allocation
            if self.stats["concurrent_allocations"] > self.stats["max_ca"]:
                self.stats["max_ca"] = self.stats["concurrent_allocations"]

            self.log.info(
                f"[SIM] Concurrent allocated requests : {self.stats['concurrent_allocations']}"
            )

            self.log.debug(summarise_ressource_catalog(self.resource_catalog))
            self.transports["visualisation"].send_multipart(
                Message.datalist(
                    [
                        ("calloc_node", f"{server_id}:{node_id}", node.sim_nb_alloc),
                        ("calloc_disk", f"{server_id}:{node_id}:{disk_id}", disk.sim_nb_alloc),
                        ("alloc", self.env.now, allocation),
                        ("calloc", self.env.now, self.stats["concurrent_allocations"]),
                    ]
                ),
                "sim",
            )

        #### Try to deallocate a request from virtual storage
        elif allocation < 0:

            if container.level - allocation < 0:
                self.log.error(
                    f"[SIM ERROR] Deallocation on {server_id}:{node_id}:{disk_id}"
                    + "causes level to get below 0. Something bad is happening"
                )
                raise ValueError("Not enough space (deallocation)")

            # DISK/NODE-related stats
            disk.sim_mean_nb_alloc += (self.env.now - disk.sim_last_alloc_time) * disk.sim_nb_alloc
            node.sim_mean_nb_alloc += (self.env.now - node.sim_last_alloc_time) * node.sim_nb_alloc
            disk.sim_mean_cap_utilisation += (self.env.now - disk.sim_last_alloc_time) * (
                (container.level * 100) / container.capacity
            )
            disk.sim_last_alloc_time = self.env.now
            node.sim_last_alloc_time = self.env.now
            container.get(-allocation)
            disk.sim_nb_alloc -= 1
            node.sim_nb_alloc -= 1

            # Global stats and real-time visualisation data #
            self.stats["concurrent_allocations"] -= 1
            self.stats["total_gb_dealloc"] += -allocation
            self.log.info(
                f"[SIM] Concurrent allocated requests : {self.stats['concurrent_allocations']}"
            )

            self.log.debug(summarise_ressource_catalog(self.resource_catalog))
            self.transports["visualisation"].send_multipart(
                Message.datalist(
                    [
                        ("calloc_node", f"{server_id}:{node_id}", node.sim_nb_alloc),
                        ("calloc_disk", f"{server_id}:{node_id}:{disk_id}", disk.sim_nb_alloc),
                        ("alloc", self.env.now, allocation),
                        ("calloc", self.env.now, self.stats["concurrent_allocations"]),
                    ]
                ),
                "sim",
            )
        else:
            # Not raising so far, because this 'error' could be generated by a
            # simple incorrect rounding on our side.
            self.log.error(
                "[SIM ERROR] Attempting to allocate/deallocate 0GB on "
                + f"{server_id}:{node_id}:{disk_id}"
            )

    def allocate(self, request: StorageRequest):
        """Simulate allocation"""

        ### BEFORE REQUEST START TIME

        self.log.debug(
            f"[SIM] Registering {request.job_id} ({request.capacity} GB on "
            + f"{request.server_id}:{request.node_id}:{request.disk_id})"
        )
        self.log.debug(
            f"[SIM] .. Duration is {request.duration} / start_time is {request.start_time}"
        )

        ### Stats on request before we know if allocation will be successful ###
        # Register any delay in allocation compared to original need from client.
        if request.start_time > request.original_start_time:
            self.stats["total_waiting_time_minutes"] += (
                request.start_time - request.original_start_time
            ).total_seconds() * 60
            self.stats["delayed_requests"] += 1

        # Stats and validation step for possible split request
        self.track_split_request(request)

        # Wait for the actual allocation time of request
        storage = yield simpy.events.Timeout(
            self.env, delay=int(request.start_time.timestamp()), value=request.capacity
        )

        ### AT REQUEST START TIME

        # Set up first event as the first allocation time on any disk:
        if self.stats["first_event_time"] == 0:
            self.stats["first_event_time"] = self.env.now

        # If one of the other subrequests have failed to be allocated,
        # they are all blacklist and no more request from this split
        # should be allocated
        if self.split_blacklisted(request):
            self.log.error(f"[SIM] Subrequest {request.job_id} blacklisted, skipping alloc")
        else:

            ### ATTEMPT THE ACTUAL ALLOCATION, AND STOP RIGHT HERE IF IT FAILS
            try:
                self.update_disk(request.server_id, request.node_id, request.disk_id, storage)
            except ValueError as exc:
                self.log.error(
                    f"[SIM] {request.job_id} not allocated to disk at {self.env.now} : {exc}"
                )
                self.invalidate_split(request)
            else:

                self.log.info(f"[SIM] {request.job_id} allocated to disk at {self.env.now}")
                self.validate_split(request)

                # Keep track of delayed requests (in allocated state)
                if request.start_time > request.original_start_time:
                    self.stats["allocated_delayed_requests"] += 1

                # Wait for deallocation time and attempt deallocation (should never fail, but who knows...)
                yield simpy.events.Timeout(self.env, delay=request.duration.seconds)

                ### AT REQUEST END TIME

                if self.split_blacklisted(request):
                    self.log.error(
                        f"[SIM] Subrequest {request.job_id} blacklisted, skipping DEalloc"
                    )
                else:
                    try:
                        self.update_disk(
                            request.server_id, request.node_id, request.disk_id, -storage
                        )
                    except ValueError as exc:
                        self.log.error(f"Failure at deallocation at {self.env.now} : {exc}")
                    else:
                        self.log.info(f"[SIM] {request.job_id} deallocated from disk")
                        self.stats["last_event_time"] = self.env.now

    def record_refused_request(self, request: StorageRequest):
        """Record a failed request during simulation (at the correct time,
        in case we want to plot it during simulation)"""

        self.log.debug(
            f"[SIM] Recording failure for {request.job_id} ({request.capacity} GB on "
            + f"{request.server_id}:{request.node_id}:{request.disk_id})"
        )

        self.log.debug(f"[SIM] Duration is {request.duration} / start_time is {request.start_time}")

        yield simpy.events.Timeout(
            self.env,
            delay=int(request.start_time.timestamp()),
        )
        self.stats["scheduler_fallbacks"] += 1
        if request.start_time > request.original_start_time:
            self.stats["refused_delayed_requests"] += 1
        if request.divided > 1:
            last_dash = request.job_id.rfind("-")
            if request.job_id[last_dash:] == "-0":
                self.stats["refused_split_requests"] += 1

    def process_request(self, request: StorageRequest):
        """Process a storage request, which might be any state"""

        if request.state is ReqState.OPENED:
            self.log.debug("New request is in OPENED state")
        elif request.state is ReqState.PENDING:
            self.log.debug("New request is in PENDING state")
        elif request.state is ReqState.GRANTED:
            self.log.debug("New request is in GRANTED state")
        elif request.state is ReqState.REFUSED:
            self.log.debug("New request is in REFUSED state")
            self.env.process(self.record_refused_request(request))
        elif request.state is ReqState.ALLOCATED:
            self.log.debug("New request is in ALLOCATED state")
            self.env.process(self.allocate(request))
        elif request.state is ReqState.FAILED:
            self.log.warning("New request is in FAILED state")
            # self.env.process(self.record_refused_request(request))
        elif request.state is ReqState.ENDED:
            self.log.debug("New request is in ENDED state")
        elif request.state is ReqState.ABORTED:
            self.log.debug("New request is in ABORTED state")
        else:
            self.log.warning("Request is in an unknown state")

    def process_registration(self, identity: bytes, node_data: dict):
        """Process new registration from server (by adding containers in the simulation)"""

        self.log.debug(f"Registration from server id : {identity}")

        for data in node_data:
            node = Node.from_dict(data)
            self.log.info(f"[PRE-SIM] Processing new node registration for node {node.hostname}")
            setattr(node, "sim_nb_alloc", 0)
            setattr(node, "sim_mean_nb_alloc", 0)
            setattr(node, "sim_last_alloc_time", 0)

            for disk in node.disks:
                # Colocate a simulation container with each disk of a node.
                self.log.info(
                    "[PRE-SIM] New simpy.Container for disk "
                    + f"{disk.uid}, with capacity {disk.capacity}"
                )
                setattr(
                    disk,
                    "sim_container",
                    simpy.Container(self.env, init=0, capacity=disk.capacity),
                )
                setattr(disk, "sim_nb_alloc", 0)  # at a given simulation time
                setattr(disk, "sim_mean_nb_alloc", 0)
                setattr(disk, "sim_last_alloc_time", 0)
                setattr(disk, "sim_mean_cap_utilisation", 0)
                setattr(disk, "sim_max_cap_utilisation", 0)
                setattr(disk, "sim_max_alloc", 0)

            self.resource_catalog.append_resources(identity, [node])

    def pre_sim_summary(self):
        """Simulation preliminary summary. Displays informations about
        the resources considered during the simulation, the number of events, etc
        """

        print("# --- Availaible resources ---")
        total_capacity = 0

        for server_id, nodes in self.resource_catalog.storage_resources.items():
            total_server_capacity = 0

            for node in nodes:
                total_node_capacity = 0
                for disk in node.disks:
                    total_node_capacity += disk.capacity
                total_server_capacity += total_node_capacity

                print(
                    f" - Server {server_id} provisionned Node {node.hostname}"
                    + f" with {len(node.disks)} disks."
                )
                print(f"    -> Total node capacity is {total_node_capacity}")

            print(f"  => Total server capacity is {total_server_capacity}")

            total_capacity += total_server_capacity
        print(f"==> Total platform capacity is {total_capacity}")

        print(f"## Number of events collected for simulation: {self.stats['requests_nb']}")

    def simulation(self):
        """Run the simulation"""

        if self.visualisation:
            # Init a visualisation app
            v_uid = f"VIS-{str(uuid.uuid4().hex)[:6]}"
            self.log.info(f"Initiating visualisation {v_uid}")
            pvis = Process(target=start_visualisation, args=(self.config_path,))
            pvis.start()
            sleep(2)

        self.pre_sim_summary()

        self.env.run()

        self.log.info(
            f"[POST-SIM] Maximum number of concurrent allocations: {self.stats['max_ca']}"
        )
        self.log.info(f"[POST-SIM] Total GB allocated: {self.stats['total_gb_alloc']}")
        self.log.info(f"[POST-SIM] Total GB deallocated: {self.stats['total_gb_dealloc']}")
        self.log.info(f"Full stats: {self.stats}")

        if self.stats["last_event_time"] == self.stats["first_event_time"]:
            sim_duration = 1
        else:
            sim_duration = self.stats["last_event_time"] - self.stats["first_event_time"]
        for server_id, node, disk in self.resource_catalog.list_resources():
            disk.sim_mean_nb_alloc /= sim_duration
            disk.sim_mean_cap_utilisation /= sim_duration
            self.log.info(
                f"Disk {server_id}:{node.uid}:{disk.uid} \n"
                f"  - allocations average = {disk.sim_mean_nb_alloc:.2f}\n"
                f"  - max allocations = {disk.sim_max_alloc} \n"
                f"  - utilisation average = {disk.sim_mean_cap_utilisation:.2f}%\n"
                f"  - max utilisation = {disk.sim_max_cap_utilisation:.2f}%"
            )

        # Output data for yml statistics file.
        sim_data = {
            "max_concurrent_allocations": self.stats["max_ca"],
            "requests_count": self.stats["requests_nb"],
            "split_requests_count": self.stats["split_requests"],
            "alloc_split_requests_count": self.stats["allocated_split_requests"],
            "failed_split_requests_count": self.stats["failed_split_requests"],
            "refused_split_requests_count": self.stats["refused_split_requests"],
            "split_threshold_gb": self.conf["block_size_gb"],
            "registrations_count": self.stats["registrations_nb"],
            "scheduler_fallbacks_count": self.stats["scheduler_fallbacks"],
            "simulation_fallbacks_count": self.stats["simulation_fallbacks"],
            "tt_delay_time_minutes": self.stats["total_waiting_time_minutes"],
            "delayed_requests_count": self.stats["delayed_requests"],
            "alloc_delayed_requests_count": self.stats["allocated_delayed_requests"],
            "refused_delayed_requests_count": self.stats["refused_delayed_requests"],
            "retries_allowed": "yes" if self.conf["allow_retry"] else "no",
            "tt_allocated_gb": self.stats["total_gb_alloc"],
            "tt_deallocated_gb": self.stats["total_gb_dealloc"],
            "sim_first_ts": self.stats["first_event_time"],
            "sim_last_ts": self.stats["last_event_time"],
            "sim_duration": sim_duration,
            "nodes": [
                {
                    "id": f"{server_id}:{node.uid}",
                    "mean_nb_alloc": round(node.sim_mean_nb_alloc / sim_duration, 3),
                    "last_alloc_ts": node.sim_last_alloc_time,
                    "disks": [
                        {
                            "id": disk.uid,
                            "capacity": disk.capacity,
                            "mean_nb_alloc": round(disk.sim_mean_nb_alloc, 3),
                            "max_alloc": disk.sim_max_alloc,
                            "last_alloc_time": disk.sim_last_alloc_time,
                            "mean_capacity_utilisation": round(disk.sim_mean_cap_utilisation, 3),
                            "max_cap_utilisation": round(disk.sim_max_cap_utilisation, 3),
                        }
                        for disk in node.disks
                    ],
                }
                for server_id, node in self.resource_catalog.list_nodes()
            ],
        }

        with open("output.yml", "w", encoding="utf-8") as output:
            yaml.dump(sim_data, output)

        if self.visualisation:
            pvis.join()

    def run(self):
        """Infinite loop for collecting events (before simulation is run)"""

        self.log.info(f"Simulation server {self.uid} ready.")
        eos = False
        stop = 3

        while stop != 0:
            try:

                events = dict(self.transports["poller"].poll(timeout=5000))

                if eos and not events:
                    # After 3 polling attempts that result in 0 events, consider that
                    # no more messages will arrive (if EoS flag has been raised as well)
                    stop -= 1
                    self.log.info(f"Polling attempt with no message ({stop}/3)")

                for _, mask in events.items():

                    if mask == zmq.POLLIN:
                        identity, message = self.transports["simulation"].recv_multipart()
                        if message.category is MsgCat.REQUEST:
                            self.process_request(self.schema.load(message.content))
                            self.stats["requests_nb"] += 1
                        elif message.category is MsgCat.REGISTRATION:
                            self.process_registration(identity[1], message.content)
                            self.stats["registrations_nb"] += 1
                        elif message.category is MsgCat.EOS:
                            self.log.info(
                                "EoS received. Simulation server will stop receiving messages ASAP."
                            )
                            eos = True
                        else:
                            self.log.warning(
                                "Undesired message category received "
                                + f"({message.category}, silently discarding"
                            )

            except zmq.ZMQError as err:
                if err.errno == zmq.ETERM:
                    break
                raise
            except KeyboardInterrupt:
                self.log.info("[!] Simulation server stopped by Ctrl-C - Now running simulation")
                break

            if self.stats["requests_nb"] % 1000 == 0:
                self.log.info(f"[PRE-SIM] Collected {self.stats['requests_nb']} events.")

        self.simulation()
        self.log.info("Simulation ended")
        self.transports["context"].destroy()
