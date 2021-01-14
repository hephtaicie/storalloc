#!/usr/bin/env python3

import sys
import logging

sys.path.append("..")
from common.resources import NodeStatus, DiskStatus

class WorstCase (object):

    def __init__ (self):
        super().__init__()

        
    def compute (self, storage_resources, new_r):
        if not storage_resources:
            return -1, -1
        
        resources_status = self._compute_status (storage_resources, new_r)
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

        return target_node.get_idx(), target_disk.get_idx()

        
    ###############################
    # Compute achievable bandwidth
    ###############################
    def _compute_status (self, storage_resources, new_request):
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
