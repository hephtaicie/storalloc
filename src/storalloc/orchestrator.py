""" Storalloc
    Default orchestrator
"""

import datetime
import time
import logging
import zmq

from storalloc.job import Job, JobStatus
from storalloc.request import Request
from storalloc.job_queue import JobQueue
from storalloc.sched_strategy import SchedStrategy
from storalloc.resources import ResourceCatalog, Node
from storalloc.config import config_from_yaml
from storalloc.message import Message, MsgCat
from storalloc.logging import get_storalloc_logger


def recv_msg(socket):
    """Receive a message on a socket"""
    message_parts = socket.recv_multipart()
    identities, data = message_parts[:-1], message_parts[-1]
    return (identities, data)


class Orchestrator:
    """Default orchestrator"""

    def __init__(self, config_path: str):
        """Init orchestrator"""

        self.log = get_storalloc_logger()
        self.conf = config_from_yaml(config_path)
        self.client_socket, self.server_socket, self.poller = self.zmq_init()
        self.pending_jobs = JobQueue()
        self.running_jobs = JobQueue()
        self.rcatalog = ResourceCatalog()
        self.scheduling_strategy = SchedStrategy()
        self.scheduling_strategy.set_strategy(self.conf["sched_strategy"])
        # self.env = simpy.Environment()

    def zmq_init(self):
        """Init ZMQ in order to be ready for connections"""

        context = zmq.Context()

        if self.conf["transport"] == "tcp":
            client_url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['client_port']}"
            server_url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['server_port']}"
        elif self.conf["transport"] == "ipc":
            client_url = f"ipc://{self.conf['orchestrator_fe_ipc']}.ipc"
            server_url = f"ipc://{self.conf['orchestrator_be_ipc']}.ipc"

        self.log.info(f"Binding socket for client on {client_url}")
        client_socket = context.socket(zmq.ROUTER)  # pylint: disable=no-member
        client_socket.bind(client_url)

        self.log.info(f"Binding socket for server on {server_url}")
        server_socket = context.socket(zmq.ROUTER)  # pylint: disable=no-member
        server_socket.bind(server_url)

        self.log.info("Creating poller for both client and server")
        poller = zmq.Poller()
        poller.register(server_socket, zmq.POLLIN)
        poller.register(client_socket, zmq.POLLIN)

        return (client_socket, server_socket, poller)

    def grant_allocation(self, job: Job, target_node: int, target_disk: int):
        """Grant a storage request and register it"""

        logging.debug(f"[{job.uid:05}] Add {job.request} on node {target_node}, disk {target_disk}")

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

    def release_allocation(self, job: Job):
        """Release a storage allocation"""
        self.log.info(f"Job<{job.uid:05}> - Release allocation")

    def run(self, simulate: bool):
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
                message = Message.unpack(data)
                client_id = identities[0]

                if message.category == MsgCat.REQUEST:
                    try:
                        req = Request(message.content)
                    except ValueError:
                        notification = Message(MsgCat.ERROR, "Wrong request")
                        self.client_socket.send_multipart([client_id, notification.pack()])
                        # notification.send(self.client_socket, client_id)
                    else:
                        job = Job(current_job_id, client_id, req, simulate)
                        current_job_id += 1

                        notification = Message(
                            MsgCat.NOTIFICATION, f"Pending job allocation {job.uid}"
                        )
                        print(notification)
                        print(notification.pack())
                        print(client_id)
                        self.client_socket.send_multipart([client_id, notification.pack()])
                        # notification.send(self.client_socket, client_id)

                        job.status = JobStatus.QUEUED
                        self.pending_jobs.add(job)

                        notification = Message(
                            MsgCat.NOTIFICATION,
                            f"job {job.uid} queued and waiting for resources",
                        )
                        self.client_socket.send_multipart([client_id, notification.pack()])
                        # notification.send(self.client_socket, client_id)
                elif message.category == MsgCat.EOS:
                    end_of_simulation = True
                    notification = Message(MsgCat.SHUTDOWN, None)
                    self.client_socket.send_multipart([client_id, notification.pack()])
                    # notification.send(self.client_socket, client_id)
                else:
                    self.log.warning(
                        "Unknown message cat. ({message.category}) received from a client"
                    )

            ################
            # SERVER SOCKET
            ################
            if self.server_socket in events:
                identities, data = recv_msg(self.server_socket)
                message = Message.unpack(data)

                if message.category == MsgCat.REGISTRATION:
                    server_id = identities[0]
                    self.rcatalog.append_resources(
                        server_id, [Node.from_dict(data) for data in message.content]
                    )
                    logging.debug("Server registered. New resources available.")
                    # TODO: setup monitoring system with newly added resources
                elif message.category == MsgCat.CONNECTION:
                    client_id = identities[1]
                    notification = Message(MsgCat.ALLOCATION, message.content)
                    self.client_socket.send_multipart([client_id, notification.pack()])
                    # notification.send(self.client_socket, client_id)
                else:
                    self.log.warning(
                        "Unknown message cat. ({message.category}) received from a server"
                    )

            ################
            # PROCESS QUEUES
            ################
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
                            job.set_pending()

            time.sleep(1)
