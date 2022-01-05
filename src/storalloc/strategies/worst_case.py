"""Storalloc
   "Worst case" scheduler
"""

import random

from storalloc.strategies.base import StrategyInterface

# pylint: disable=logging-fstring-interpolation,logging-not-lazy


class WorstCase(StrategyInterface):
    """Worst Case scheduler"""

    def compute(self, resource_catalog, request):
        """Compute worst case allocation"""

        self._compute_status(resource_catalog, request)
        # resources_status.sort(key=lambda x: x.bw, reverse=True)

        candidates = []

        for server_id, node in resource_catalog.list_nodes():

            self.log.debug(f"[WCc] Disks before filtering: {len(node.disks)} candidates")
            filtered_disks = [
                disk for disk in node.disks if disk.disk_status.capacity > request.capacity
            ]
            self.log.debug(f"[WCc] Disks after filtering: {len(filtered_disks)} candidates")
            if not filtered_disks:
                continue
            sorted_disks = sorted(filtered_disks, key=lambda d: d.disk_status.bandwidth)
            best_bandwidth = sorted_disks[0].disk_status.bandwidth
            for disk in sorted_disks:
                if disk.disk_status.bandwidth < best_bandwidth:
                    break
                candidates.append((server_id, node.uid, disk.uid))

        nb_candidates = len(candidates)
        if nb_candidates:
            self.log.info(f"There are {nb_candidates} to choose from")
            return random.choice(candidates)

        self.log.error("Not enough space on any of the disks")
        return ("", -1, -1)

    def _compute_status(self, resource_catalog, request):
        """Compute achievable bandwidth"""

        start_time_chunk = request.start_time.timestamp()  # datetime to seconds timestamp
        end_time_chunk = request.end_time.timestamp()
        self.log.debug(
            "[WC] Entering _compute_status."
            + f"Request expects allocation between ts {start_time_chunk} AND {end_time_chunk}"
        )

        current_node = None
        node_bw = 0.0
        for server_id, node, disk in resource_catalog.list_resources():

            self.log.debug(f"[WC] Analysing disk {server_id}:{node.uid}:{disk.uid}")

            if current_node != node:
                node.node_status.bandwidth = (
                    node_bw / (request.end_time - request.start_time).seconds / len(node.disks)
                )
                self.log.info(f"[WC] Access bandwidth for {server_id}:{node.uid} = {node_bw} GB/s")
                node_bw = 0.0
                current_node = node

            # Update worst case bandwidth for current disk based on possibly concurrent allocations
            disk_bw = 0.0
            overlap_offset = self._compute_allocation_overlap(disk, node, request, node_bw, disk_bw)

            node_bw += (end_time_chunk - start_time_chunk + overlap_offset) * node.bandwidth
            disk_bw += (end_time_chunk - start_time_chunk + overlap_offset) * disk.write_bandwidth
            disk_bw = disk_bw / ((request.end_time - request.start_time).seconds)
            self.log.debug(f"[WC] .. Disk/Current node_bw: {node_bw}")
            self.log.debug(f"[WC] .. Disk/Current disk_bw: {disk_bw}")

            disk.disk_status.bandwidth = disk_bw
            disk.disk_status.capacity = disk.capacity
            self.log.info(
                f"[WC] Access bandwidth for disk {server_id}:{node.uid}:{disk.uid}"
                + f" => {disk.capacity} GB / {disk_bw} GB/s"
            )

    def _compute_allocation_overlap(self, disk, node, request, node_bw, disk_bw):
        """For a given disk, check for overlapping allocations and resulting worst case bandwidth"""

        num_allocations = len(disk.allocations)
        self.log.debug(f"[WC] .. This disk currently has {num_allocations} allocs")
        overlap_offset = 0

        for idx, allocation in enumerate(disk.allocations):

            # Allocations are sorted on a per-disk basis upon insertion.
            if allocation.end_time < request.start_time:
                self.log.debug(
                    f"[WC] .. Alloc {idx} ends at {allocation.end_time}."
                    + " That's before our request's allocation starts."
                    + " Skipping this allocation"
                )
                continue

            # Allocations that may overlap with current request
            overlap_duration = request.overlaps(allocation)  # time in "seconds.microseconds"
            if overlap_duration != 0.0:
                # overlapping_allocations += 1
                self.log.debug(
                    f"[WC] .. Alloc {idx} and our request's alloc overlap for {overlap_duration}s"
                )
                overlap_requests = num_allocations - idx + 1
                self.log.debug(
                    f"[WC] .. Worst case, there are {overlap_requests} allocations "
                    + "overlapping with our request"
                )

                overlap_offset += overlap_duration  # formerly an update to start_time_chunk

                node_bw += (overlap_duration * node.bandwidth) / overlap_requests
                disk_bw += (overlap_duration * node.bandwidth) / overlap_requests
                disk.status.capacity -= allocation.capacity

        return overlap_offset
