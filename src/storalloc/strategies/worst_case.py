"""Storalloc
   "Worst case" scheduler
"""

from storalloc.strategies.base import StrategyInterface


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
                sorted_disks = sorted(filtered_disks, key=lambda d: d.disk_status.bandwidth)
                self.log.debug(f"[WCc] Disks after sorting: {len(sorted_disks)}")
                return (server_id, node.uid, sorted_disks[0].uid)

        self.log.error("Not enough space on any of the disks")
        return (None, None, None)

    def _compute_status(self, resource_catalog, request):
        """Compute achievable bandwidth"""

        self.log.debug("[WC] Entering _compute_status")

        # Main loop computing allocation cost per box and per disk
        for server_id, nodes in resource_catalog.storage_resources.items():

            for node in nodes:

                node_bw = 0.0

                for disk in node.disks:

                    self.log.debug(f"[WC] Analysing disk {server_id}:{node.uid}:{disk.uid}")

                    start_time_chunk = (
                        request.start_time.timestamp()
                    )  # datetime to seconds timestamp
                    num_allocations = len(disk.allocations)
                    disk_capacity = disk.capacity
                    disk_bw = 0.0
                    self.log.debug(
                        f"[WC] This request expects allocation at {start_time_chunk} s (timestamp)"
                    )

                    self.log.debug(f"[WC] .. This disk currently has {num_allocations} allocs")

                    for idx, allocation in enumerate(disk.allocations):

                        if allocation.end_time < request.start_time:
                            self.log.debug(
                                f"[WC] .. Alloc {idx} ends before our request's allocation starts, skipping"
                            )
                            break

                        overlap = request.overlaps(allocation)  # time in "seconds.microseconds"
                        if overlap != 0.0:
                            self.log.debug(
                                f"[WC] .. Alloc {idx} and our request's alloc overlap for {overlap} seconds."
                            )
                            overlap_reqs = num_allocations - idx + 1
                            start_time_chunk += overlap

                            node_bw += overlap * node.bandwidth / overlap_reqs
                            disk_bw += overlap * disk.write_bandwidth / overlap_reqs
                            disk_capacity -= allocation.capacity
                            self.log.debug(f"[WC] .. A/Current node_bw: {node_bw}")
                            self.log.debug(f"[WC] .. A/Current disk_bw: {disk_bw}")
                            self.log.debug(f"[WC] .. A/Current disk_capa: {disk_capacity}")

                    node_bw += (request.end_time.timestamp() - start_time_chunk) * node.bandwidth
                    disk_bw += (
                        request.end_time.timestamp() - start_time_chunk
                    ) * disk.write_bandwidth
                    disk_bw = disk_bw / ((request.end_time - request.start_time).seconds)
                    self.log.debug(f"[WC] .. Disk/Current node_bw: {node_bw}")
                    self.log.debug(f"[WC] .. Disk/Current disk_bw: {disk_bw}")

                    disk.disk_status.bandwidth = disk_bw
                    disk.disk_status.capacity = disk_capacity
                    self.log.info(
                        f"[WC] Access bandwidth for disk {server_id}:{node.uid}:{disk.uid}"
                        + f" => {disk_capacity} GB / {disk_bw} GB/s"
                    )

                node.node_status.bandwidth = (
                    node_bw / (request.end_time - request.start_time).seconds / len(node.disks)
                )
                self.log.info(f"[WC] Access bandwidth for {server_id}:{node.uid} = {node_bw} GB/s")
