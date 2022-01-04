"""Storalloc
   "Worst case" scheduler
"""

from storalloc.strategies.base import StrategyInterface

# pylint: disable=logging-fstring-interpolation,logging-not-lazy


class WorstCase(StrategyInterface):
    """Worst Case scheduler"""

    def compute(self, resource_catalog, request):
        """Compute worst case allocation"""

        self._compute_status(resource_catalog, request)
        # resources_status.sort(key=lambda x: x.bw, reverse=True)

        for server_id, nodes in resource_catalog.storage_resources.items():

            for node in nodes:

                self.log.debug(f"[WCc] Disks before filtering: {len(node.disks)}")
                filtered_disks = [
                    disk for disk in node.disks if disk.disk_status.capacity > request.capacity
                ]
                self.log.debug(f"[WCc] Disks after filtering: {len(filtered_disks)}")
                if not filtered_disks:
                    continue
                sorted_disks = sorted(filtered_disks, key=lambda d: d.disk_status.bandwidth)
                self.log.debug(f"[WCc] Disks after sorting: {len(sorted_disks)}")
                if not sorted_disks:
                    continue
                return (server_id, node.uid, sorted_disks[0].uid)

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
        for server_id, node, disk in resource_catalog.list_resources():

            self.log.debug(f"[WC] Analysing disk {server_id}:{node.uid}:{disk.uid}")

            if current_node != node:
                node_bw = 0.0
                # node.node_status.bandwidth = (
                #     node_bw / (request.end_time - request.start_time).seconds / len(node.disks)
                # )
                # self.log.info(f"[WC] Access bandwidth for {server_id}:{node.uid} = {node_bw} GB/s")
                current_node = node

            # Update worst case bandwidth for current disk based on possibly concurrent allocations
            disk_bw = 0.0
            self._compute_allocation_overlap(disk, node, request, node_bw, disk_bw)

            node_bw += (end_time_chunk - start_time_chunk) * node.bandwidth
            disk_bw += (end_time_chunk - start_time_chunk) * disk.write_bandwidth
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
        overlapping_allocations = 0

        for idx, allocation in enumerate(disk.allocations):

            # Allocations are sorted on a per-disk basis upon insertion.
            if allocation.end_time < request.start_time:
                self.log.debug(
                    f"[WC] .. Alloc {idx} ends at {allocation.end_time}."
                    + " That's before our request's allocation starts."
                    + " Skipping following allocations"
                )
                break

            # Allocations that may overlap with current request
            overlap = request.overlaps(allocation)  # time in "seconds.microseconds"
            if overlap != 0.0:
                overlapping_allocations += 1
                self.log.debug(
                    f"[WC] .. Alloc {idx} and our request's alloc overlap for {overlap}s"
                )

                # New avail bandwidth is previously known bandwidth (for the same request)
                # Worst case considers that any overlap means allocation and request overlap
                # for the entire duration of the request
                disk.status.bandwidth = (
                    (end_time_chunk - start_time_chunk) * disk.bandwidth
                ) / overlapping_allocations

                node_bw += overlap * node.bandwidth / overlap_reqs
                disk_bw += overlap * disk.write_bandwidth / overlap_reqs
                disk.status.capacity -= allocation.capacity
                self.log.debug(f"[WC] .. A/Current node_bw: {node_bw}")
                self.log.debug(f"[WC] .. A/Current disk_bw: {disk_bw}")
                self.log.debug(f"[WC] .. A/Current disk_capa: {disk_capacity}")

        return True if overlapping_allocations else False
