#!/usr/bin/env python3

import os
import yaml
import sys
import argparse
import logging
import zmq
import time
import simpy
import datetime

from src.job import Job
from src.request import Request
from src.job_queue import JobQueue
from src.sched_strategy import SchedStrategy
from src.resources import ResourceCatalog, NodeStatus, DiskStatus
from src.config_file import ConfigFile
from src.message import Message

# Global variables
conf_file = None
simulate = False
pending_jobs = JobQueue()
running_jobs = JobQueue()
resource_catalog = ResourceCatalog()
scheduling_strategy = SchedStrategy()


def parse_args():
    """Parse arguments given as input on the command line"""
    global conf_file, simulate

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path of the StorAlloc configuration file (YAML)")
    parser.add_argument(
        "--simulate", help="Simulation mode for replaying traces", action="store_true"
    )
    parser.add_argument("-v", "--verbose", help="Display debug information", action="store_true")

    args = parser.parse_args()

    if not args.config:
        parser.print_usage()
        print("Error: argument --config (-c) is mandatory!")
        sys.exit(1)
    else:
        conf_file = ConfigFile(args.config)

    if args.simulate:
        simulate = True

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")


def client_bind_url():
    return (
        "tcp://"
        + conf_file.get_orch_client_bind_ipv4()
        + ":"
        + str(conf_file.get_orch_client_bind_port())
    )


def server_bind_url():
    return (
        "tcp://"
        + conf_file.get_orch_server_bind_ipv4()
        + ":"
        + str(conf_file.get_orch_server_bind_port())
    )


def recv_msg(socket):
    """Receive a message on a socket"""
    message_parts = socket.recv_multipart()
    identities, data = message_parts[:-1], message_parts[-1]
    return identities, data


def grant_allocation(job, server_socket, client_socket, target_node, target_disk):
    global pending_jobs, running_jobs, resource_catalog

    logging.debug(
        "["
        + str(job.id()).zfill(5)
        + "] Add "
        + job.request.to_string()
        + " on node "
        + str(target_node)
        + ", disk "
        + str(target_disk)
    )

    alloc_request = {
        "job_id": job.id(),
        "disk": target_disk,
        "capacity": job.request.capacity(),
        "duration": job.request.duration(),
    }
    identities = [resource_catalog.identity_of_node(target_node), job.client_identity()]
    notification = Message("allocate", alloc_request)
    notification.send(server_socket, identities)

    job.set_allocated()
    running_jobs.add(job)
    pending_jobs.remove(job)

    resource_catalog.add_allocation(target_node, target_disk, job)
    resource_catalog.print_status(target_node, target_disk)

    notification = Message("notification", f"Granted job allocation {job.id()}")
    notification.send(client_socket, job.client_identity())


def release_allocation(job, server_socket, client_socket):
    print("[" + str(job.id()).zfill(5) + "] Release allocation")


def simulate_scheduling(job, server_socket, client_socket, earliest_start_time):
    yield env.timeout(job.sim_start_time(earliest_start_time))

    target_node, target_disk = scheduling_strategy.compute(resource_catalog, job)

    # If a disk on a node has been found, we allocate the request
    if target_node >= 0 and target_disk >= 0:
        grant_allocation(job, server_socket, client_socket, target_node, target_disk)
    else:
        print("[" + str(job.id()).zfill(5) + "] Unable to allocate request. Exiting...")
        sys.exit(1)

    # Duration + Fix seconds VS minutes
    yield env.timeout(job.sim_start_time())


def main(argv):
    """Main loop"""
    global pending_jobs, running_jobs, resource_catalog, scheduling_strategy

    parse_args()

    context = zmq.Context()

    client_socket = context.socket(zmq.ROUTER)
    client_socket.bind(client_bind_url())

    server_socket = context.socket(zmq.ROUTER)
    server_socket.bind(server_bind_url())

    poller = zmq.Poller()
    poller.register(server_socket, zmq.POLLIN)
    poller.register(client_socket, zmq.POLLIN)

    current_job_id = 0

    # Simulation mode
    end_of_simulation = False

    scheduling_strategy.set_strategy(conf_file.get_orch_strategy())

    while True:
        events = dict(poller.poll(100))

        ################
        # CLIENT SOCKET
        ################
        if client_socket in events:
            identities, data = recv_msg(client_socket)
            message = Message.from_packed_message(data)
            client_id = identities[0]

            if message.get_type() == "request":
                try:
                    req = Request(message.get_content())
                except ValueError:
                    notification = Message("error", "Wrong request")
                    notification.send(client_socket, client_id)
                else:
                    job = Job(current_job_id, client_id, req, simulate)
                    current_job_id += 1

                    notification = Message("notification", f"Pending job allocation {job.id()}")
                    notification.send(client_socket, client_id)

                    job.set_queued()
                    pending_jobs.add(job)

                    notification = Message(
                        "notification", f"job {job.id()} queued and waiting for resources"
                    )
                    notification.send(client_socket, client_id)
            elif message.get_type() == "eos":
                end_of_simulation = True
                notification = Message("shutdown", None)
                notification.send(client_socket, client_id)
            else:
                print("[W] Wrong message type received from a client")

        ################
        # SERVER SOCKET
        ################
        if server_socket in events:
            identities, data = recv_msg(server_socket)
            message = Message.from_packed_message(data)

            if message.get_type() == "register":
                server_id = identities[0]
                resource_catalog.append_resources(server_id, message.get_content())
                logging.debug("Server registered. New resources available.")
                # TODO: setup monitoring system with newly added resources
            elif message.get_type() == "connection":
                client_id = identities[1]
                notification = Message("allocation", message.get_content())
                notification.send(client_socket, client_id)
            else:
                print("[W] Wrong message type received from a server")

        ################
        # PROCESS QUEUES
        ################
        if simulate:
            if end_of_simulation:
                sim_env = simpy.Environment()

                earliest_start_time = datetime.datetime.now()
                latest_end_time = datetime.datetime(1970, 1, 1)

                pending_jobs.sort_asc_start_time()

                for job in pending_jobs:
                    if job.start_time() < earliest_start_time:
                        earliest_start_time = job.start_time()
                    if job.end_time() > latest_end_time:
                        latest_end_time = job.end_time()

                sim_duration = (latest_end_time - earliest_start_time).total_seconds() + 1

                for job in pending_jobs:
                    env.process(simulate_scheduling())

                env.run(until=sim_duration)
        else:
            for job in pending_jobs:
                target_node, target_disk = scheduling_strategy.compute(resource_catalog, job)

                # If a disk on a node has been found, we allocate the request
                if target_node >= 0 and target_disk >= 0:
                    grant_allocation(job, server_socket, client_socket, target_node, target_disk)
                else:
                    if not job.is_pending():
                        logging.debug(
                            "["
                            + str(job.id()).zfill(5)
                            + "] Unable to allocate incoming request for now"
                        )
                        job.set_pending()

        time.sleep(1)


if __name__ == "__main__":
    main(sys.argv[1:])
