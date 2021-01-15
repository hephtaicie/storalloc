#!/usr/bin/env python3

import os
import yaml
import sys
import argparse
import logging
import zmq
import time

from src.job import Job
from src.request import Request
from src.job_queue import JobQueue
from src.sched_strategy import SchedStrategy

sys.path.append("..")
from common.resources import ResourceCatalog, NodeStatus, DiskStatus
from common.config_file import ConfigFile
from common.message import Message

# Default values
conf_file = None


def parse_args ():
    """Parse arguments given as input on the command line"""
    global conf_file

    parser = argparse.ArgumentParser ()
    parser.add_argument ('-c', '--config', help="Path of the StorAlloc configuration file (YAML)")
    parser.add_argument ('-v', '--verbose', help="Display debug information", action='store_true')

    args = parser.parse_args ()

    if not args.config:
        parser.print_usage()
        print ('Error: argument --config (-c) is mandatory!')
        sys.exit(1)
    else:
        conf_file = ConfigFile(args.config)

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")
        

def client_bind_url ():
    return ("tcp://"+conf_file.get_orch_client_bind_ipv4()
            +":"+str(conf_file.get_orch_client_bind_port()))

            
def server_bind_url ():
    return ("tcp://"+conf_file.get_orch_server_bind_ipv4()
            +":"+str(conf_file.get_orch_server_bind_port()))


def recv_msg (socket):
    """Receive a message on a socket"""
    message_parts = socket.recv_multipart()
    identities, data = message_parts[:-1], message_parts[-1]
    return identities, data


def main (argv):
    """Main loop

    """
    
    parse_args ()

    context = zmq.Context()

    client_socket = context.socket(zmq.ROUTER)
    client_socket.bind(client_bind_url())

    server_socket = context.socket(zmq.ROUTER)
    server_socket.bind(server_bind_url())


    poller = zmq.Poller()
    poller.register(server_socket, zmq.POLLIN)
    poller.register(client_socket, zmq.POLLIN)

    #TODO: Keep the state in a file and allow loading it at startup
    pending_jobs = JobQueue ()
    running_jobs = JobQueue ()

    resource_catalog    = ResourceCatalog ()
    scheduling_strategy = SchedStrategy ()

    scheduling_strategy.set_strategy (conf_file.get_orch_strategy())

    while True:
        events = dict(poller.poll(100))

        if client_socket in events:
            identities, data  = recv_msg(client_socket)
            message           = Message.from_packed_message (data)
            client_id         = identities[0]
            
            if message.get_type () == "request":
                try:
                    req = Request (message.get_content())
                except ValueError:
                    notification = Message ("error", "Wrong request")
                    notification.send (client_socket, client_id)
                else:                
                    job = Job (pending_jobs.count() + running_jobs.count(), identities[0], req)
                    
                    notification = Message ("notification", f"Pending job allocation {job.id()}")
                    notification.send (client_socket, client_id)

                    job.set_queued()
                    pending_jobs.add (job)

                    notification = Message ("notification", f"job {job.id()} queued and waiting for resources")
                    notification.send (client_socket, client_id)
            else:
                print ("[W] Wrong message type received from a client")

        if server_socket in events:
            identities, data  = recv_msg(server_socket)
            message           = Message.from_packed_message (data)
            
            if message.get_type () == "register":
                server_id = identities[0]
                resource_catalog.append_resources (server_id, message.get_content ())
                logging.debug ('Server registered. New resources available.')
            elif message.get_type () == "connection":
                client_id    = identities[1]
                notification = Message ("allocation", message.get_content())
                notification.send (client_socket, client_id)
            else:
                print ("[W] Wrong message type received from a server")
                
        """Process the requests in queue"""
        for job in pending_jobs:
            
            target_node, target_disk = scheduling_strategy.compute (resource_catalog, job)

            # If a disk on a node has been found, we allocate the request
            if target_node >= 0 and target_disk >= 0:
                logging.debug ("["+str(job.id()).zfill(5)+"] Add "+job.request.to_string()+
                               " on node "+str(target_node)+", disk "+str(target_disk))

                # Submit request to the selected node
                alloc_request = {"job_id":job.id(), "disk":target_disk,
                                 "capacity":job.request.capacity(), "duration":job.request.duration()}
                identities    = [resource_catalog.identity_of_node(target_node), job.client_identity()]
                notification  = Message ("allocate", alloc_request)
                notification.send (server_socket, identities)

                job.set_allocated ()
                running_jobs.add (job)
                pending_jobs.remove (job)
                
                resource_catalog.add_allocation (target_node, target_disk, job)
                resource_catalog.print_status (target_node, target_disk)

                notification = Message ("notification", f"Granted job allocation {job.id()}")
                notification.send (client_socket, job.client_identity())
            else:
                if not job.is_pending():
                    logging.debug ("["+str(job.id()).zfill(5)+"] Unable to allocate incoming request for now")
                    job.set_pending()
            
        time.sleep (1)
   

if __name__ == "__main__":
    main (sys.argv[1:])
