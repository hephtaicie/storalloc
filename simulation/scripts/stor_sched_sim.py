#!/usr/bin/env python3

import sys
import argparse
import logging
import datetime as dt
import numpy as np

# Default values
n_nodes = 1
n_disks_per_node = 10
disk_capacity = 1500  # GB
n_requests = 5
box_bw = 12.5  # GBps
disk_w_bw = 3.2  # GBps


################
# Request class
################
class Request(object):
    def __init__(self, capacity, duration):
        super().__init__()
        self._capacity = capacity
        self._duration = duration
        self._start_time = dt.datetime.now(dt.timezone.utc) + dt.timedelta(
            minutes=np.random.randint(180)
        )
        self._end_time = self._start_time + dt.timedelta(minutes=self._duration)

    def print_request(self):
        print(self.request_string())

    def request_string(self):
        return (
            "["
            + str(self._capacity)
            + " GB, "
            + str(self._duration)
            + " m, "
            + str(self._start_time)
            + ", "
            + str(self._end_time)
            + "]"
        )

    def get_capacity(self):
        return self._capacity

    def get_start_time(self):
        return self._start_time

    def get_end_time(self):
        return self._end_time

    def get_duration(self):
        return self._duration

    def get_timediff(self):
        return self._end_time - self._start_time


###########################
# Classes for NVMeoF nodes
###########################
class Disk(object):
    def __init__(self, idx):
        super().__init__()

        self._idx = idx
        self._bw = disk_w_bw
        self._capacity = disk_capacity

        self.allocs = list()

    def get_idx(self):
        return self._idx

    def get_bw(self):
        return self._bw

    def get_capacity(self):
        return self._capacity


class NVMeoFNode(object):
    def __init__(self, idx):
        super().__init__()

        self._idx = idx
        self._bw = box_bw

        self.disks = list()

    def get_idx(self):
        return self._idx

    def get_bw(self):
        return self._bw


###############################
# Classes for node/disk status
###############################
class DiskStatus(object):
    def __init__(self, idx):
        super().__init__()
        self._idx = idx
        self.bw = 0.0
        self.capacity = disk_capacity

    def get_idx(self):
        return self._idx


class NVMeoFNodeStatus(object):
    def __init__(self, idx):
        super().__init__()
        self._idx = idx
        self.bw = 0.0
        self.disk_status = list()

    def get_idx(self):
        return self._idx


###############################
# Parse command-line arguments
###############################
def parse_args():
    global n_nodes, n_disks_per_node, disk_capacity, n_requests

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-b", "--boxes", type=int, help="Number of NVMeoF boxes (default: " + str(n_nodes) + ")"
    )
    parser.add_argument(
        "-d",
        "--disks",
        type=int,
        help="Number of NVMe diskes per box (default: " + str(n_disks_per_node) + ")",
    )
    parser.add_argument(
        "-c",
        "--capacity",
        type=int,
        help="Capacity per disk in GB (default: " + str(disk_capacity) + ")",
    )
    parser.add_argument(
        "-r",
        "--requests",
        type=int,
        help="Number of requests to simulate (default: " + str(n_requests) + ")",
    )
    parser.add_argument("-v", "--verbose", help="Display debug information", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")

    if args.boxes:
        n_nodes = args.boxes
    if args.disks:
        n_disks_per_node = args.disks
    if args.capacity:
        disk_capacity = args.capacity
    if args.requests:
        n_requests = args.requests


##############################################
# ASCII-based output of the given scheduling
##############################################
def print_status(storage_resources, target_node_id, target_disk_id):
    # Concatenate lists of requests per disk to determine ealiest start time and latest end time
    all_requests_list = list()
    for n in range(0, len(storage_resources)):
        for d in range(0, len(storage_resources[n].disks)):
            all_requests_list.extend(storage_resources[n].disks[d].allocs)

    earliest_request = min([x.get_start_time() for x in all_requests_list])
    latest_request = max([x.get_end_time() for x in all_requests_list])
    steps = int((latest_request - earliest_request).seconds / 300)  # granularity: 5 minutes

    # Print the current status of the scheduling on nodes and disks
    for n in range(0, len(storage_resources)):
        print("┌───┬", end="")
        for s in range(0, steps):
            print("─", end="")
        print()
        for d in range(0, len(storage_resources[n].disks)):
            if not storage_resources[n].disks[d].allocs:
                print("│" + str(d).rjust(3) + "│")
            else:
                for i, r in enumerate(storage_resources[n].disks[d].allocs):
                    if i == 0:
                        print("│" + str(d).rjust(3) + "│", end="")
                    else:
                        print("│   │", end="")
                    offset = int((r.get_start_time() - earliest_request).seconds / 300)
                    for o in range(0, offset):
                        print(" ", end="")
                    req_time = int((r.get_end_time() - r.get_start_time()).seconds / 300)
                    req_time = 1 if req_time == 0 else req_time
                    for j in range(0, req_time):
                        if (
                            target_node_id == n
                            and target_disk_id == d
                            and i == len(storage_resources[n].disks[d].allocs) - 1
                        ):
                            print("□", end="")
                        else:
                            print("■", end="")
                    print()
            if d < len(storage_resources[n].disks) - 1:
                print("├---┼", end="")
                for s in range(0, steps):
                    print("-", end="")
                print()
        print("└───┴", end="")
        for s in range(0, steps):
            print("─", end="")
        print()


###############################
# Compute achievable bandwidth
###############################
def compute_status(storage_resources, new_request):
    # Initialiaze structures for achievable bandwidths (worst-case scenario)
    resources_status = list()

    for n in range(0, len(storage_resources)):
        node_status = NVMeoFNodeStatus(n)
        for d in range(0, len(storage_resources[n].disks)):
            disk_status = DiskStatus(d)
            node_status.disk_status.append(disk_status)
        resources_status.append(node_status)

    # Main loop computing allocation cost per box and per disk
    for n in range(0, len(storage_resources)):
        node = storage_resources[n]
        node_idx = node.get_idx()
        node_bw = 0.0
        for d in range(0, len(node.disks)):
            start_time_chunk = new_request.get_start_time()
            disk = node.disks[d]
            disk_idx = disk.get_idx()
            disk_bw = 0.0
            disk_capacity = disk.get_capacity()
            disk_allocs = disk.allocs

            if disk_allocs:
                num_allocs = len(disk_allocs)
                disk_allocs.sort(key=lambda x: x.get_end_time())

                for a in range(0, num_allocs):
                    alloc = disk_allocs[a]
                    if alloc.get_end_time() > start_time_chunk:
                        overlap_time = min(
                            alloc.get_end_time() - start_time_chunk,
                            new_request.get_end_time() - start_time_chunk,
                        )
                        overlap_reqs = num_allocs - a + 1

                        start_time_chunk += overlap_time

                        node_bw += overlap_time.seconds * node.get_bw() / overlap_reqs
                        disk_bw += overlap_time.seconds * disk.get_bw() / overlap_reqs
                        disk_capacity -= alloc.get_capacity()

            node_bw += (new_request.get_end_time() - start_time_chunk).seconds * node.get_bw()
            disk_bw += (new_request.get_end_time() - start_time_chunk).seconds * disk.get_bw()
            disk_bw = disk_bw / (
                (new_request.get_end_time() - new_request.get_start_time()).seconds
            )

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

        node_bw = (
            node_bw
            / ((new_request.get_end_time() - new_request.get_start_time()).seconds)
            / len(node.disks)
        )

        resources_status[n].bw = node_bw
        logging.debug("Access bandwidth for box " + str(n) + ": %.2f GBps" % resources_status[n].bw)

    return resources_status


################
# Main function
################
def main(argv):
    parse_args()

    print("Set up:")
    print("   - " + str(n_nodes) + " NVMeoF nodes")
    print(
        "   - "
        + str(n_disks_per_node)
        + " NVMe disks of "
        + str(disk_capacity)
        + " GB each per box"
    )
    print("   - Total capacity: " + str(n_nodes * n_disks_per_node * disk_capacity) + " GB")
    print("   - " + str(n_requests) + " requests simulated")

    # Initialize available storage resources
    storage_resources = list()

    for n in range(0, n_nodes):
        new_node = NVMeoFNode(n)
        for d in range(0, n_disks_per_node):
            new_disk = Disk(d)
            new_node.disks.append(new_disk)
        storage_resources.append(new_node)

    # Generate requests based on normal distribution of capacities and durations
    requests = list()
    req_capacities = [int(abs(i) + 5) for i in np.random.normal(500, 200, n_requests)]
    req_durations = [int(abs(i) + 5) for i in np.random.normal(120, 90, n_requests)]

    for r in range(0, n_requests):
        new_request = Request(req_capacities[r], req_durations[r])
        requests.append(new_request)

    requests.sort(key=lambda x: x.get_start_time())

    # Simulate the scheduling for the list of fake requests
    for new_r in requests:
        logging.debug("New incoming request: " + new_r.request_string())

        ##############################
        # Load balancing algorithm   #
        ##############################
        resources_status = compute_status(storage_resources, new_r)

        # Pick up the most suitable box and disk for this request
        req_allocated = False
        resources_status.sort(key=lambda x: x.bw, reverse=True)

        target_disk = None
        target_node = None
        for b in range(0, len(storage_resources)):
            current_node = resources_status[b]

            current_node.disk_status.sort(key=lambda x: x.bw, reverse=True)
            for disk in current_node.disk_status:
                if (disk.capacity - new_r.get_capacity()) < 0:
                    logging.debug(
                        "Not enough remaining space on node "
                        + str(current_node.get_idx())
                        + ", disk "
                        + str(disk.get_idx())
                        + " (req: "
                        + str(new_r.get_capacity())
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

            if target_disk is None:
                logging.debug(
                    "Unable to allocate the current request on node " + str(current_node.get_idx())
                )

        # If a disk on a node has been found, we allocate the request
        if target_disk is not None:
            logging.debug(
                "Add request "
                + new_r.request_string()
                + " on node "
                + str(target_node.get_idx())
                + ", disk "
                + str(target_disk.get_idx())
            )
            storage_resources[target_node.get_idx()].disks[target_disk.get_idx()].allocs.append(
                new_r
            )
            print_status(storage_resources, target_node.get_idx(), target_disk.get_idx())
        else:
            logging.debug("Request " + new_r.request_string() + " cannot be allocated!")

        print("-------")


if __name__ == "__main__":
    main(sys.argv[1:])
