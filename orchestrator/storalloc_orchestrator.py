#!/usr/bin/env python3

import os
import yaml
import sys
import argparse
import logging
import zmq
import time

from src.request import Request
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
        

def parse_client_request (req):
    """Parse comma-separated request received by a client"""
    try:
        capacity = int(req.split(',')[0])
        duration = int(req.split(',')[1])
    except:
        capacity = 0
        duration = 0

    return capacity, duration


def client_bind_url ():
    return ("tcp://"+conf_file.get_orch_client_bind_ipv4()
            +":"+str(conf_file.get_orch_client_bind_port()))

            
def server_bind_url ():
    return ("tcp://"+conf_file.get_orch_server_bind_ipv4()
            +":"+str(conf_file.get_orch_server_bind_port()))



def recv_msg(socket):
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
    allocated_requests = list ()
    job_id_to_identity = dict ()
    awaiting_requests  = list ()

    resource_catalog    = ResourceCatalog ()
    scheduling_strategy = SchedStrategy ()

    scheduling_strategy.set_strategy (conf_file.get_orch_strategy())

    while True:
        events = dict(poller.poll(100))

        if client_socket in events:
            identities, data   = recv_msg(client_socket)
            message            = Message.from_packed_message (data)
            capacity, duration = parse_client_request (message.get_content())
           
            if capacity <= 0 or duration <= 0:
                reply = Message ("error", "Wrong request")
                client_socket.send_multipart ([identities[0], reply.pack()])
            else:
                job_idx = len(awaiting_requests) + len (allocated_requests)
                new_r   = Request (job_idx, identities[0], capacity, duration)
                logging.debug ("["+str(new_r.get_idx()).zfill(5)+"] New incoming request: "+new_r.request_string())
                                
                reply = Message ("notification", "Pending job allocation "+str(new_r.get_idx()))
                client_socket.send_multipart ([identities[0], reply.pack()])

                new_r.set_status ("queued")
                awaiting_requests.append (new_r)
                
                reply = Message ("notification", "job "+str(new_r.get_idx())+" queued and waiting for resources")
                client_socket.send_multipart ([identities[0], reply.pack()])

        if server_socket in events:
            identities, data   = recv_msg(server_socket)
            message            = Message.from_packed_message (data)
            
            if message.get_type () == "register":
                resource_catalog.append_resources (identities[0], message.get_content ())
                logging.debug ('Server registered. New resources available.')
            elif message.get_type () == "connection":
                message = Message ("allocation", message.get_content())
                job_id  = message.get_content()["job_id"]
                client_socket.send_multipart ([job_id_to_identity[job_id], message.pack()])
                
        """Process the requests in queue"""
        for req in awaiting_requests:
            
            target_node, target_disk = scheduling_strategy.compute (resource_catalog.get_resources_list (), req)

            # If a disk on a node has been found, we allocate the request
            if target_node >= 0 and target_disk >= 0:
                job_id = new_r.get_idx()
                logging.debug ("["+str(job_id).zfill(5)+"] Add "+req.request_string()+
                               " on node "+str(target_node)+", disk "+str(target_disk))

                # Submit request to the selected node
                alloc_request = {"job_id":job_id, "disk":target_disk,
                                 "capacity":new_r.get_capacity(), "duration":new_r.get_duration()}
                message = Message ("allocate", alloc_request)
                server_socket.send_multipart ([resource_catalog.get_identity_of_node(target_node), message.pack()])

                req.set_status ("allocated")
                allocated_requests.append (req)
                awaiting_requests.remove (req)
                job_id_to_identity [job_id] = req.get_identity()

                resource_catalog.add_allocation (target_node, target_disk, req)
                resource_catalog.print_status (target_node, target_disk)

                message = Message ("notification", "Granted job allocation "+str(job_id))
                client_socket.send_multipart ([req.get_identity(), message.pack()])
            else:
                if req.get_status () != "awaiting":
                    logging.debug ("["+str(req.get_idx()).zfill(5)+"] Unable to allocate incoming request for now")
                    req.set_status ("awaiting")
            
        time.sleep (1)
   

if __name__ == "__main__":
    main (sys.argv[1:])
