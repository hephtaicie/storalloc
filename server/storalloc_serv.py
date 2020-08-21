#!/usr/bin/env python3

import os
import yaml
import socket
import sys
import argparse
import logging
import time
import parted
from src.nvmeof_node import NVMeoFNode
from src.nvme_disk import NVMeDisk
from src.request import Request
from src.resource_status import DiskStatus, NodeStatus
from src.nvmet import nvme

HOST  = '148.187.104.85'
PORT  = 6666
sock  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conf  = ''
reset = False

def parse_args ():
    global conf, reset

    parser = argparse.ArgumentParser ()
    parser.add_argument ('-c', '--config', help="Configuration file (YAML) describing the available ressources")
    parser.add_argument ('-r', '--reset', help="Reset the existing NVMeoF configuration", action='store_true')
    parser.add_argument ('-v', '--verbose', help="Display debug information", action='store_true')

    args = parser.parse_args ()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")

    if args.reset:
        reset = True

    if not args.config:
        parser.print_usage()
        print ('Error: argument --config (-c) is mandatory!')
        sys.exit(1)
    elif not os.path.exists (args.config):
        print ('Error: the configuration file given as input does not exist!')
        sys.exit(1)
    else:
        conf = args.config


def load_config (config_file):
    stream = open (config_file, 'r')
    content = yaml.safe_load (stream)
    stream.close ()

    storage_resources = list ()
    for idx_n, node in enumerate(content['hosts']):
        new_node = NVMeoFNode (idx_n, node)
        for idx_d, disk in enumerate (node['disks']):
            new_disk = NVMeDisk (idx_d, disk)
            new_node.disks.append (new_disk)
        storage_resources.append (new_node)
        
    return storage_resources


def parse_request (req):
    try:
        capacity = int(req.decode('utf-8').split(',')[0])
        duration = int(req.decode('utf-8').split(',')[1])
    except:
        capacity = 0
        duration = 0

    return capacity, duration

###############################
# Compute achievable bandwidth
###############################
def compute_status (storage_resources, new_request):
    # Initialiaze structures for achievable bandwidths (worst-case scenario)
    resources_status = list ()
                    
    for n in range (0, len(storage_resources)):
        node_status = NodeStatus (n)
        for d in range (0, len(storage_resources[n].disks)):
            disk_status = DiskStatus (d, storage_resources[n].disks[d].get_capacity())
            node_status.disk_status.append (disk_status)
        resources_status.append (node_status)
            
    # Main loop computing allocation cost per box and per disk                
    for n in range (0, len(storage_resources)):
        node     = storage_resources[n]
        node_idx = node.get_idx()
        node_bw  = 0.0
        for d in range (0, len(node.disks)):
            start_time_chunk = new_request.get_start_time ()
            disk             = node.disks[d]
            disk_idx         = disk.get_idx()
            disk_bw          = 0.0
            disk_capacity    = disk.get_capacity()
            disk_allocs      = disk.allocs
            
            if disk_allocs:
                num_allocs = len (disk_allocs)
                disk_allocs.sort (key=lambda x: x.get_end_time())
        
                for a in range(0, num_allocs):
                    alloc = disk_allocs[a]
                    if alloc.get_end_time() > start_time_chunk:
                        overlap_time = min (alloc.get_end_time() - start_time_chunk, new_request.get_end_time() - start_time_chunk)
                        overlap_reqs = num_allocs - a + 1
                
                        start_time_chunk  += overlap_time

                        node_bw        += overlap_time.seconds * node.get_bw() / overlap_reqs
                        disk_bw        += overlap_time.seconds * disk.get_bw() / overlap_reqs
                        disk_capacity  -= alloc.get_capacity ()

            node_bw += (new_request.get_end_time () - start_time_chunk).seconds * node.get_bw()
            disk_bw += (new_request.get_end_time () - start_time_chunk).seconds * disk.get_bw()
            disk_bw  = disk_bw / ((new_request.get_end_time () - new_request.get_start_time ()).seconds)

            resources_status[n].disk_status[d].bw       = disk_bw
            resources_status[n].disk_status[d].capacity = disk_capacity
            logging.debug ("Access bandwidth for disk "+str(disk_idx)+" (node "+str(node_idx)+", "+
                           str(disk_capacity)+" GB): %.2f GBps" % disk_bw)

        node_bw = node_bw / ((new_request.get_end_time () - new_request.get_start_time ()).seconds) / len(node.disks)

        resources_status[n].bw = node_bw
        logging.debug ("Access bandwidth for box "+str(n)+": %.2f GBps" % resources_status[n].bw)

    return resources_status


##############################################
# ASCII-based output of the given scheduling
##############################################
def print_status (storage_resources, target_node_id, target_disk_id):
    # Concatenate lists of requests per disk to determine ealiest start time and latest end time
    all_requests_list = list ()
    for n in range (0, len(storage_resources)):
        for d in range (0, len(storage_resources[n].disks)):
            all_requests_list.extend (storage_resources[n].disks[d].allocs)

    earliest_request = min([x.get_start_time() for x in all_requests_list])
    latest_request   = max([x.get_end_time() for x in all_requests_list])
    steps            = int((latest_request - earliest_request).seconds / 300) # granularity: 5 minutes

    # Print the current status of the scheduling on nodes and disks
    for n in range (0, len(storage_resources)):
        print ("┌───┬", end ="")
        for s in range (0, steps):
            print ("─", end ="")
        print ()
        for d in range (0, len(storage_resources[n].disks)):
            if not storage_resources[n].disks[d].allocs:
                print ("│"+str(d).rjust(3)+"│")
            else:
                for i, r in enumerate(storage_resources[n].disks[d].allocs):
                    if i == 0:
                        print ("│"+str(d).rjust(3)+"│", end="")
                    else:
                        print ("│   │", end="")
                    offset = int((r.get_start_time() - earliest_request).seconds / 300)
                    for o in range (0, offset):
                        print (" ", end="")
                    req_time = int((r.get_end_time() - r.get_start_time ()).seconds / 300)
                    req_time = 1 if req_time == 0 else req_time
                    for j in range (0, req_time):
                        if target_node_id == n and target_disk_id == d and i == len(storage_resources[n].disks[d].allocs) - 1:
                            print ("□", end="")
                        else:
                            print ("■", end="")
                    print ()
            if d < len(storage_resources[n].disks) - 1:
                print ("├---┼", end="")
                for s in range (0, steps):
                    print ("-", end ="")
                print()
        print ("└───┴", end ="")
        for s in range (0, steps):
            print ("─", end ="")
        print ()


##########################
# Reset storage resources
##########################
def reset_resources (storage_resources):
    nvme.Root().clear_existing()
    for node in storage_resources:
        for disk in node.disks:
            dev_nvme  = parted.getDevice(disk.get_blk_dev())
            dev_nvme.clobber ()
            disk_nvme = parted.freshDisk(dev_nvme, "gpt")
            disk_nvme.commit ()
    logging.debug ('Reset of storage resources done!')

            
################
# Main function
################
def main (argv):
    global sock

    if os.getuid() != 0:
        print ('Error: this script must be run with root privileges!')
        sys.exit(1)
    
    parse_args ()

    storage_resources = load_config (conf)
    requests = list () #TODO: Keep the state in a file and allow loading it at startup

    sock.bind ((HOST, PORT))
    sock.listen ()

    if reset:
        confirm = ""
        while confirm not in ["y", "n"]:
            confirm = input("Are you sure you want to reset the existing NVMeoF configuration, including disk partitions [Y/N]? ").lower()
        if confirm == "y":
            reset_resources (storage_resources)
        else:
            sys.exit(1)
    
    while True:
        conn, addr = sock.accept()
        logging.debug ('Connection accepted from '+addr[0]+':'+str(addr[1]))
        while True:
            data = conn.recv(1024)
            if not data:
                break
            
            capacity, duration = parse_request (data)

            if capacity > 0 and duration > 0:
                new_r = Request (len(requests), capacity, duration)

                logging.debug ("New incoming request: "+new_r.request_string())
                conn.send(bytearray('Pending job allocation '+str(new_r.get_idx()), 'utf-8'))

                resources_status = compute_status (storage_resources, new_r)

                # Pick up the most suitable box and disk for this request
                req_allocated = False
                resources_status.sort (key=lambda x: x.bw, reverse = True)

                target_disk = None
                target_node = None
                for b in range (0, len(storage_resources)):
                    current_node = resources_status[b]
        
                    current_node.disk_status.sort (key=lambda x: x.bw, reverse = True)
                    for disk in current_node.disk_status:
                        if (disk.capacity - new_r.get_capacity()) < 0:
                            logging.debug ("Not enough remaining space on node "+str(current_node.get_idx())+
                                           ", disk "+str(disk.get_idx())+
                                           " (req: "+str(new_r.get_capacity())+" GB, "+
                                           "avail: "+str(disk.capacity)+" GB)")
                        else:
                            if (target_disk is None or disk.bw > target_disk.bw) and current_node.bw >= disk.bw:
                                target_node = current_node
                                target_disk = disk    
                                break
                
                # If a disk on a node has been found, we allocate the request
                if target_disk is not None:
                    # Queue job
                    logging.debug ("Add "+new_r.request_string()+" on node "+str(target_node.get_idx())+", disk "+str(target_disk.get_idx()))
                    conn.send(bytearray('job '+str(new_r.get_idx())+' queued and waiting for resources', 'utf-8'))

                    # Allocation resources
                    nqn, port = new_r.allocate_resources (
                        storage_resources[target_node.get_idx()].get_ipv4(),
                        storage_resources[target_node.get_idx()].disks[target_disk.get_idx()].get_blk_dev())
                    conn.send(bytearray('nqn '+nqn+' '+str(port), 'utf-8'))
                    conn.send(bytearray('job '+str(new_r.get_idx())+' has been allocated resources', 'utf-8'))

                    # Update and print running queue
                    storage_resources[target_node.get_idx()].disks[target_disk.get_idx()].allocs.append (new_r)
                    requests.append (new_r)
                    print_status (storage_resources, target_node.get_idx(), target_disk.get_idx())
                    conn.send(bytearray('Granted job allocation '+str(new_r.get_idx()), 'utf-8'))
                else:
                    logging.debug ("Request "+new_r.request_string()+" cannot be allocated!")
                    conn.send(bytearray('Unable to allocate ressources for job '+str(new_r.get_idx()), 'utf-8'))

                time.sleep (1)
                conn.send(bytearray('EOT', 'utf-8'))
            else:
                conn.close()
                logging.debug ('Client disconnected')
   

if __name__ == "__main__":
    main (sys.argv[1:])
