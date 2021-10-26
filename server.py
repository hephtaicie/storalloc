#!/usr/bin/env python3

import os
import sys
import argparse
import logging
import zmq
import time

# from src.nvmet import nvme

from src.resources import ResourceCatalog
from src.config_file import ConfigFile
from src.message import Message

# Default values
conf_file = None
reset = False
simulate = False
resource_catalog = ResourceCatalog()


def parse_args():
    """Parse arguments given as input on the command line"""
    global conf_file, reset, simulate, resource_catalog

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path of the StorAlloc configuration file (YAML)")
    parser.add_argument("-s", "--system", help="Path of the storage system description (YAML)")
    parser.add_argument(
        "-r", "--reset", help="Reset the existing storage configurations", action="store_true"
    )
    parser.add_argument(
        "--simulate",
        help="Receive requests only. No actual storage allocation",
        action="store_true",
    )
    parser.add_argument("-v", "--verbose", help="Display debug information", action="store_true")

    args = parser.parse_args()

    if not args.config:
        parser.print_usage()
        print("Error: argument --config (-c) is mandatory!")
        sys.exit(1)
    else:
        conf_file = ConfigFile(args.config)

    if not args.system:
        parser.print_usage()
        print("Error: argument --system (-s) is mandatory!")
        sys.exit(1)
    else:
        resource_catalog = ResourceCatalog.from_yaml(args.system)

    if args.reset:
        reset = True

    if args.simulate:
        simulate = True

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")


def reset_resources(storage_resources):
    """Reset storage configurations

    TODO: To move to a Storage class
    """

    confirm = ""
    while confirm not in ["y", "n"]:
        confirm = input(
            "Are you sure you want to reset the existing NVMeoF configuration"
            + ", including disk partitions [Y/N]? "
        ).lower()
    if confirm == "y":
        nvme.Root().clear_existing()
        for node in storage_resources:
            for disk in node.disks:
                dev_nvme = parted.getDevice(disk.get_blk_dev())
                dev_nvme.clobber()
                disk_nvme = parted.freshDisk(dev_nvme, "gpt")
                disk_nvme.commit()
        logging.debug("Reset of storage resources done!")
    else:
        sys.exit(1)


def orchestrator_url():
    return "tcp://" + conf_file.get_orch_ipv4() + ":" + str(conf_file.get_orch_port())


def main(argv):
    """Main loop

    Connect to the orchestrator, register (send available resources) and wait for allocation requests.
    """

    parse_args()

    if not simulate:
        if os.getuid() != 0:
            print("Error: this script must be run with root privileges in a non-simulated mode!")
            sys.exit(1)

    context = zmq.Context()
    sock = context.socket(zmq.DEALER)
    sock.connect(orchestrator_url())

    # reset_resources (storage_resources)

    logging.debug("Registering to the orchestrator (" + orchestrator_url() + ")")
    message = Message("register", resource_catalog.get_resources_list())
    sock.send(message.pack())

    while True:
        client_id, data = sock.recv_multipart()
        message = Message.from_packed_message(data)

        if message.get_type() == "allocate":
            print("storalloc: " + str(message.get_content()))
            job_id = message.get_content()["job_id"]
            connection = {
                "job_id": job_id,
                "type": "nvme",
                "nqn": "nqn.2014-08.com.vendor:nvme:nvm-subsystem-sn-d78432",
            }
            message = Message("connection", connection)
            sock.send_multipart([client_id, message.pack()])
        elif message.get_type() == "deallocate":
            print("storalloc: " + message.get_content())
        elif message.get_type() == "error":
            print("storalloc: [ERR] " + message.get_content())
            break
        elif message.get_type() == "shutdown":
            print("storalloc: closing the connection at the orchestrator's initiative")
            break

    time.sleep(1)

    sock.close(linger=0)
    context.term()


if __name__ == "__main__":
    main(sys.argv[1:])
