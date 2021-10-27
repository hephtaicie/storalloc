""" Storalloc
    Default orchestrator
"""

import datetime
import time
import sys
import logging
import zmq
import simpy

from storalloc.job import Job
from storalloc.request import Request
from storalloc.job_queue import JobQueue
from storalloc.sched_strategy import SchedStrategy
from storalloc.resources import ResourceCatalog
from storalloc.config_file import ConfigFile
from storalloc.message import Message


def recv_msg(socket):
    """Receive a message on a socket"""
    message_parts = socket.recv_multipart()
    identities, data = message_parts[:-1], message_parts[-1]
    return (identities, data)


def zmq_init(conf: ConfigFile):
    """Init ZMQ in order to be ready for connections"""

    context = zmq.Context()

    client_socket = context.socket(zmq.ROUTER)  # pylint: disable=no-member
    client_socket.bind(
        f"tcp://{conf.get_orch_client_bind_ipv4()}:{conf.get_orch_client_bind_port()}"
    )

    server_socket = context.socket(zmq.ROUTER)  # pylint: disable)no-member
    server_socket.bind(
        f"tcp://{conf.get_orch_server_bind_ipv4()}:{conf.get_orch_server_bind_port()}"
    )

    poller = zmq.Poller()
    poller.register(server_socket, zmq.POLLIN)
    poller.register(client_socket, zmq.POLLIN)

    return (client_socket, server_socket, poller)


class Orchestrator:
    """Default orchestrator"""

    def __init__(self, config_file: str, simulate: bool):
        """Init orchestrator"""

        self.conf = ConfigFile(config_file)
        self.simulate = simulate
        self.client_socket, self.server_socket, self.poller = zmq_init(self.conf)
        self.pending_jobs = JobQueue()
        self.running_jobs = JobQueue()
        self.resource_catalog = ResourceCatalog()
        self.scheduling_strategy = SchedStrategy()
        self.scheduling_strategy.set_strategy(self.conf.get_orch_strategy())
        self.env = simpy.Environment()

    def grant_allocation(self, job, target_node, target_disk):
        """Grant a storage request and register it"""

        logging.debug(f"[{job.uid:05}] Add {job.request} on node {target_node}, disk {target_disk}")

        alloc_request = {
            "job_id": job.uid,
            "disk": target_disk,
            "capacity": job.request.capacity,
            "duration": job.request.duration,
        }
        identities = [
            self.resource_catalog.identity_of_node(target_node),
            job.client_identity,
        ]
        notification = Message("allocate", alloc_request)
        notification.send(self.server_socket, identities)

        job.set_allocated()
        self.running_jobs.add(job)
        self.pending_jobs.remove(job)

        self.resource_catalog.add_allocation(target_node, target_disk, job)
        self.resource_catalog.print_status(target_node, target_disk)

        notification = Message("notification", f"Granted job allocation {job.uid}")
        notification.send(self.client_socket, job.client_identity)

    def release_allocation(self, job: Job):
        """Release a storage allocation"""
        print(f"[{job.uid:05}] Release allocation")

    def simulate_scheduling(self, job, earliest_start_time):
        """Simpy"""

        yield self.env.timeout(job.sim_start_time(earliest_start_time))

        target_node, target_disk = self.scheduling_strategy.compute(self.resource_catalog, job)

        # If a disk on a node has been found, we allocate the request
        if target_node >= 0 and target_disk >= 0:
            self.grant_allocation(job, target_node, target_disk)
        else:
            print(f"[{job.uid:05}] Unable to allocate request. Exiting...")
            sys.exit(1)

        # Duration + Fix seconds VS minutes
        yield self.env.timeout(job.sim_start_time())

    def run(self):
        """Init and start an orchestrator"""

        current_job_id = 0

        # Simulation mode
        end_of_simulation = False

        while True:

            events = dict(self.poller.poll(100))

            ################
            # CLIENT SOCKET
            ################
            if self.client_socket in events:

                identities, data = recv_msg(self.client_socket)
                message = Message.from_packed_message(data)
                client_id = identities[0]

                if message.type == "request":
                    try:
                        req = Request(message.content)
                    except ValueError:
                        notification = Message("error", "Wrong request")
                        notification.send(self.client_socket, client_id)
                    else:
                        job = Job(current_job_id, client_id, req, self.simulate)
                        current_job_id += 1

                        notification = Message("notification", f"Pending job allocation {job.uid}")
                        notification.send(self.client_socket, client_id)

                        job.set_queued()
                        self.pending_jobs.add(job)

                        notification = Message(
                            "notification",
                            f"job {job.uid} queued and waiting for resources",
                        )
                        notification.send(self.client_socket, client_id)
                elif message.type == "eos":
                    end_of_simulation = True
                    notification = Message("shutdown", None)
                    notification.send(self.client_socket, client_id)
                else:
                    print("[W] Wrong message type received from a client")

            ################
            # SERVER SOCKET
            ################
            if self.server_socket in events:
                identities, data = recv_msg(self.server_socket)
                message = Message.from_packed_message(data)

                if message.type == "register":
                    server_id = identities[0]
                    self.resource_catalog.append_resources(server_id, message.content)
                    logging.debug("Server registered. New resources available.")
                    # TODO: setup monitoring system with newly added resources
                elif message.type == "connection":
                    client_id = identities[1]
                    notification = Message("allocation", message.content)
                    notification.send(self.client_socket, client_id)
                else:
                    print("[W] Wrong message type received from a server")

            ################
            # PROCESS QUEUES
            ################
            if self.simulate:
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
                    target_node, target_disk = self.scheduling_strategy.compute(
                        self.resource_catalog, job
                    )

                    # If a disk on a node has been found, we allocate the request
                    if target_node >= 0 and target_disk >= 0:
                        self.grant_allocation(job, target_node, target_disk)
                    else:
                        if not job.is_pending():
                            logging.debug(
                                f"[{job.uid:05}] Currently unable to allocate incoming request"
                            )
                            job.set_pending()

            time.sleep(1)
