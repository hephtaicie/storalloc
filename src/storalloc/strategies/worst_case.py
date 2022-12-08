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

        self.log.debug(f"[WORST_CASE] Received NEW request {request}")

        # Update resource catalog (status of disks and nodes) according to the new req
        self.__compute_status(resource_catalog, request)

        candidates = []

        # Select best disks from every node
        for server_id, node in resource_catalog.list_nodes():

            self.log.debug(f"[WC:compute] Looking for candidates in NODE::{node.uid}")

            self.log.debug(f"[WC:compute] {len(node.disks)} disks before filtering")
            filtered_disks = [
                disk for disk in node.disks if disk.disk_status.capacity >= request.capacity
            ]
            self.log.debug(f"[WC:compute] {len(filtered_disks)} disks after filtering")
            # Add every not filtered out disk to the candidates
            candidates.extend([(server_id, node, disk) for disk in filtered_disks])

        if not candidates:
            self.log.debug(
                "[WC:compute] Not enough space on any of the disks,"
                + f"cannot grant request {request.job_id}"
            )
            return ("", -1, -1)

        # Sort candidates and pick the best
        sorted_disks = sorted(candidates, key=lambda t: -t[2].disk_status.bandwidth)
        best_bandwidth = sorted_disks[0][2].disk_status.bandwidth
        scd_best_bandwidth = sorted_disks[1][2].disk_status.bandwidth
        self.log.debug(f"[WC:compute] Best bandwidth among disks for this node : {best_bandwidth}")
        self.log.debug(f"[WC:compute] 2nd best for this node : {scd_best_bandwidth}")
        final_choices = []
        for server_id, node, disk in sorted_disks:
            if disk.disk_status.bandwidth < best_bandwidth:
                break
            final_choices.append((server_id, node, disk))

        self.log.debug(
            f"[WC:compute] There are {len(final_choices)} final candidate(s) to choose from"
        )
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

            self.__compute_worst_node_disk_bandwidth(node, request)

            self.log.debug(
                f"[WC] .. Access bandwidth for {server_id}:{node.uid}"
                + f"= {node.node_status.bandwidth} GB/s per disk"
            )

    def __compute_worst_node_disk_bandwidth(self, node, request):
        """Compute worst case bw for node and disks"""

        disks_overlaps = []

        for disk in node.disks:

            worst_mean_disk_bw, overlap_duration = self.__compute_worst_disk_bw(disk, request)
            disk.disk_status.bandwidth = worst_mean_disk_bw

            # Remember that this disk has at least one running allocation that could
            # compete for node bw
            if overlap_duration != 0:
                disks_overlaps.append((overlap_duration, disk.read_bandwidth))

        nb_overlap_disks = len(disks_overlaps)
        max_agg_bw = 0

        for disk_data in disks_overlaps:
            max_agg_bw += disk_data[1]  # sum bw of all active disks

        if max_agg_bw < node.bandwidth:
            # Node bw doen't throttle disks
            worst_case_mean_node_bandwidth = node.bandwidth - max_agg_bw
        else:
            # Node bw may throttle disk. Each gets an equal part of the
            # node network bandwidth, which may be > or < to the own disk
            # bw, depending on the number of disks per node
            worst_case_mean_node_bandwidth = node.bandwidth / nb_overlap_disks

        node.node_status.bandwidth = worst_case_mean_node_bandwidth
        # Throttle disk bandwidth according to node capacity
        for disk in node.disks:
            disk.disk_status.bandwidth = min(
                disk.disk_status.bandwidth, worst_case_mean_node_bandwidth
            )

    def __compute_worst_disk_bw(self, disk, request):
        """List all requests overlapping with our new request"""

        num_allocations = len(disk.allocations)
        self.log.debug(f"[WC_lo] .. This disk currently has {num_allocations} on-going allocations")

        overlap_durations = []
        disk.disk_status.capacity = disk.capacity  # Reset 'status' value to full capacity

        # Loop through allocations (from
        for idx, allocation in enumerate(disk.allocations[::-1]):

            # Note : when adding allocation on a disk, allocations are always
            # sorted by end_time (from near future ending to ending in far future)
            self.log.debug(f"[WC:disk] .. Allocation {idx}, end time : {allocation.end_time}")

            if allocation.end_time <= request.start_time:
                self.log.debug(
                    f"[WC:disk] .. Alloc {idx} ends at {allocation.end_time}."
                    + " That's before our request's allocation starts."
                    + " We can now stop"
                )
                break

            # Allocations that may overlap with current request
            overlap_duration = request.overlaps(allocation)  # time in "seconds.microseconds"
            if overlap_duration != 0.0:
                self.log.debug(
                    f"[WC:disk] .. Alloc {idx} and our request's alloc "
                    + "overlap for {overlap_duration}s"
                )

                overlap_durations.append(overlap_duration)
                disk.disk_status.capacity -= allocation.capacity
                disk.disk_status.capacity = min(disk.disk_status.capacity, 0)

        self.log.debug(f"[WC:disk] We have {len(overlap_durations)} overlapping requests")
        self.log.debug(f"[WC:disk] Overlap durations: {overlap_durations}")

        if overlap_durations:
            num_overlaps = len(overlap_durations) + 1  # +1 for our request
            max_overlap_duration = max(overlap_durations)
            self.log.debug(f"[WC:disk] Max overlap duration: {max_overlap_duration}")

            request_duration_s = request.duration.total_seconds()

            worst_case_mean_disk_bandwidth = (
                (request_duration_s - max_overlap_duration) * disk.write_bandwidth
                + (max_overlap_duration * (disk.write_bandwidth / num_overlaps))
            ) / request_duration_s
            worst_case_mean_disk_bandwidth = round(worst_case_mean_disk_bandwidth, 3)

            self.log.debug(
                f"[WC:disk] Mean worst_case disk bandwidth = {worst_case_mean_disk_bandwidth}"
            )

            return (worst_case_mean_disk_bandwidth, max_overlap_duration)

        return (disk.write_bandwidth, 0)
