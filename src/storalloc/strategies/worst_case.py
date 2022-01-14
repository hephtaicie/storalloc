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

        self.__compute_status(resource_catalog, request)
        # resources_status.sort(key=lambda x: x.bw, reverse=True)

        candidates = []

        # Select best disks from every node
        for server_id, node in resource_catalog.list_nodes():

            self.log.debug(f"[WCc] Analysing candidates for node {node.uid}")

            self.log.debug(f"[WCc] Disks before filtering: {len(node.disks)} candidates")
            filtered_disks = [
                disk for disk in node.disks if disk.disk_status.capacity > request.capacity
            ]
            self.log.debug(f"[WCc] Disks after filtering: {len(filtered_disks)} candidates")
            # Add every not filtered out disk to the candidates
            candidates.extend([(server_id, node, disk) for disk in filtered_disks])

        if not candidates:
            self.log.error("Not enough space on any of the disks")
            return ("", -1, -1)

        sorted_disks = sorted(candidates, key=lambda t: -t[2].disk_status.bandwidth)
        best_bandwidth = sorted_disks[0][2].disk_status.bandwidth
        self.log.debug(f"[WCc] Best bandwidth among disks for this node : {best_bandwidth}")
        final_choices = []
        for server_id, node, disk in sorted_disks:
            if disk.disk_status.bandwidth < best_bandwidth:
                break
            final_choices.append((server_id, node, disk))

        self.log.debug(f"There are {len(final_choices)} final candidate(s) to choose from")
        choice = random.choice(final_choices)
        return (choice[0], choice[1].uid, choice[2].uid)

    def __compute_status(self, resource_catalog, request):
        """Compute achievable bandwidth"""

        start_time_chunk = request.start_time.timestamp()  # datetime to seconds timestamp
        end_time_chunk = request.end_time.timestamp()
        self.log.debug(
            "[WC] Entering _compute_status."
            + f"Request expects allocation between ts {start_time_chunk} and {end_time_chunk}"
        )

        for server_id, node in resource_catalog.list_nodes():

            node_bw = 0.0

            for disk in node.disks:

                self.log.debug(f"[WC] Analysing disk {server_id}:{node.uid}:{disk.uid}")

                # Update worst case bandwidth for current disk based on possibly
                # concurrent allocations
                overlap_offset, tmp_node_bw, disk_bw = self.__compute_allocation_overlap(
                    disk, node, request
                )
                node_bw += tmp_node_bw

                node_bw += (end_time_chunk - (start_time_chunk + overlap_offset)) * node.bandwidth
                disk_bw += (
                    end_time_chunk - (start_time_chunk + overlap_offset)
                ) * disk.write_bandwidth
                disk_bw = disk_bw / ((request.end_time - request.start_time).seconds)
                self.log.debug(f"[WC] .. Disk/Current node_bw: {node_bw}")
                self.log.debug(f"[WC] .. Disk/Current disk_bw: {disk_bw}")

                disk.disk_status.bandwidth = disk_bw
                self.log.debug(
                    "[WC] .. Access bandwidth and max avail. capacity for disk "
                    + f"{server_id}:{node.uid}:{disk.uid}"
                    + f" => {disk.capacity} GB / {disk_bw} GB/s"
                )

            node.node_status.bandwidth = (
                node_bw / (request.end_time - request.start_time).seconds / len(node.disks)
            )
            self.log.debug(
                f"[WC] .. Access bandwidth for {server_id}:{node.uid}"
                + f"= {node.node_status.bandwidth} GB/s"
            )

    def __compute_allocation_overlap(self, disk, node, request):
        """For a given disk, loop through existing allocations and compute a
        worst case achievable bandwidth for our new request.

        - Only the allocations that end AFTER our new requests starts are considered.
        - The overlap time between an existing allocation and our new request is exact,
          but the max number of overlapping requests is a worst case scenario

        Returns the overlap_offset, which can be used to compute how long the new
        request will possibly run without overlaps.
        Returns temporary node and disk 'bandwidth' (or rather the amount of
        bytes that could possibly get through during the overlap time).
        Also silently updates the maximum capacity, considering existing allocations
        (again, worst case scenario : we consider the maximum free capacity if all allocations
        are concurrent at some point in time)
        """

        num_allocations = len(disk.allocations)
        self.log.debug(f"[WC] .. This disk currently has {num_allocations} allocs")
        overlap_offset = 0
        node_bw = 0
        disk_bw = 0
        disk.disk_status.capacity = disk.capacity

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
                self.log.debug(f"[WC] .. Temp overlap node_bw={node_bw}")
                disk_bw += (overlap_duration * disk.write_bandwidth) / overlap_requests
                self.log.debug(f"[WC] .. Temp overlap disk_bw={disk_bw}")
                disk.disk_status.capacity -= allocation.capacity
                self.log.debug(f"[WC] .. Disk capacity is {disk.disk_status.capacity}")

        return (overlap_offset, node_bw, disk_bw)
