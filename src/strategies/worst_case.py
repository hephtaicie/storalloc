#!/usr/bin/env python3

import sys
import logging

from src.resources import NodeStatus, DiskStatus


class WorstCase(object):
    def __init__(self):
        super().__init__()

    def compute(self, resource_catalog, job):
        resources_status = self._compute_status(resource_catalog, job)
        req_allocated = False
        resources_status.sort(key=lambda x: x.bw, reverse=True)

        target_disk = None
        target_node = None
        for b in range(0, resource_catalog.node_count()):
            current_node = resources_status[b]

            current_node.disk_status.sort(key=lambda x: x.bw, reverse=True)
            for disk in current_node.disk_status:
                if (disk.capacity - job.request.capacity()) < 0:
                    logging.debug(
                        "Not enough remaining space on node "
                        + str(current_node.get_idx())
                        + ", disk "
                        + str(disk.get_idx())
                        + " (req: "
                        + str(job.request.capacity())
                        + " GB, "
                        + "avail: "
                        + str(disk.capacity)
                        + " GB)"
                    )
                else:
                    if (
                        target_disk is None or disk.bw > target_disk.bw
                    ) and current_node.bw >= disk.bw:
                        target_node = current_node
                        target_disk = disk
                        break

        return target_node.get_idx(), target_disk.get_idx()

    ###############################
    # Compute achievable bandwidth
    ###############################
    def _compute_status(self, resource_catalog, job):
        # Initialiaze structures for achievable bandwidths (worst-case scenario)
        resources_status = list()

        for n in range(0, resource_catalog.node_count()):
            node_status = NodeStatus(n)
            for d in range(0, resource_catalog.disk_count(n)):
                disk_status = DiskStatus(d, resource_catalog.disk_capacity(n, d))
                node_status.disk_status.append(disk_status)
            resources_status.append(node_status)

        # Main loop computing allocation cost per box and per disk
        for n in range(0, resource_catalog.node_count()):
            node = resource_catalog.get_node(n)
            node_idx = node.get_idx()
            node_bw = 0.0
            for d in range(0, resource_catalog.disk_count(n)):
                start_time_chunk = job.start_time()
                disk = node.disks[d]
                disk_idx = disk.get_idx()
                disk_bw = 0.0
                disk_capacity = disk.get_capacity()
                disk_allocs = disk.allocs

                if disk_allocs:
                    num_allocs = len(disk_allocs)
                    disk_allocs.sort(key=lambda x: x.end_time())

                    for a in range(0, num_allocs):
                        alloc = disk_allocs[a]
                        if alloc.end_time() > start_time_chunk:
                            overlap_time = min(
                                alloc.end_time() - start_time_chunk,
                                job.end_time() - start_time_chunk,
                            )
                            overlap_reqs = num_allocs - a + 1

                            start_time_chunk += overlap_time

                            node_bw += overlap_time.seconds * node.get_bw() / overlap_reqs
                            disk_bw += overlap_time.seconds * disk.get_bw() / overlap_reqs
                            disk_capacity -= alloc.request.capacity()

                node_bw += (job.end_time() - start_time_chunk).seconds * node.get_bw()
                disk_bw += (job.end_time() - start_time_chunk).seconds * disk.get_bw()
                disk_bw = disk_bw / ((job.end_time() - job.start_time()).seconds)

                resources_status[n].disk_status[d].bw = disk_bw
                resources_status[n].disk_status[d].capacity = disk_capacity
                logging.debug(
                    "Access bandwidth for disk "
                    + str(disk_idx)
                    + " (node "
                    + str(node_idx)
                    + ", "
                    + str(disk_capacity)
                    + " GB): %.2f GBps" % disk_bw
                )

            node_bw = node_bw / ((job.end_time() - job.start_time()).seconds) / len(node.disks)

            resources_status[n].bw = node_bw
            logging.debug(
                "Access bandwidth for box " + str(n) + ": %.2f GBps" % resources_status[n].bw
            )

        return resources_status
