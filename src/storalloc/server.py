""" Storalloc
    Default server
"""

import time
import os
import sys
import logging
import zmq

# from storalloc.nvmet import nvme

from storalloc.resources import ResourceCatalog
from storalloc.config_file import ConfigFile
from storalloc.message import Message

# def reset_resources(storage_resources):
#    """Reset storage configurations
#
#    TODO: To move to a Storage class
#    """
#
#    confirm = ""
#    while confirm not in ["y", "n"]:
#        confirm = input(
#            "Are you sure you want to reset the existing NVMeoF configuration"
#            + ", including disk partitions [Y/N]? "
#        ).lower()
#    if confirm == "y":
#        nvme.Root().clear_existing()
#        for node in storage_resources:
#            for disk in node.disks:
#                dev_nvme = parted.getDevice(disk.get_blk_dev())
#                dev_nvme.clobber()
#                disk_nvme = parted.freshDisk(dev_nvme, "gpt")
#                disk_nvme.commit()
#        logging.debug("Reset of storage resources done!")
#    else:
#        sys.exit(1)


def zmq_init(url: str):
    """Init ZeroMQ socket"""

    context = zmq.Context()
    sock = context.socket(zmq.DEALER)
    sock.connect(url)

    return (context, sock)


def run(config_file, system, reset, simulate):
    """Server main loop
    Connect to the orchestrator, register (send available resources)
    and wait for allocation requests.
    """

    if not simulate and os.getuid() != 0:
        print(
            "Error: this script must be run with root privileges in a non-simulated mode!"
        )
        sys.exit(1)

    conf = ConfigFile(config_file)

    resource_catalog = ResourceCatalog.from_yaml(system)

    orchestrator_url = f"tcp://{conf.get_orch_ipv4()}:{conf.get_orch_port()}"
    context, sock = zmq_init(orchestrator_url)

    # if reset:
    #   reset_resources(storage_resources)

    logging.debug(f"Registering to the orchestrator ({orchestrator_url})")
    message = Message("register", resource_catalog.get_resources_list())
    sock.send(message.pack())

    while True:
        client_id, data = sock.recv_multipart()
        message = Message.from_packed_message(data)

        if message.get_type() == "allocate":
            print(f"storalloc: {message.get_content()}")
            job_id = message.get_content()["job_id"]
            connection = {
                "job_id": job_id,
                "type": "nvme",
                "nqn": "nqn.2014-08.com.vendor:nvme:nvm-subsystem-sn-d78432",
            }
            message = Message("connection", connection)
            sock.send_multipart([client_id, message.pack()])
        elif message.get_type() == "deallocate":
            print(f"storalloc: {message.get_content()}")
        elif message.get_type() == "error":
            print(f"storalloc: [ERR] {message.get_content()}")
            break
        elif message.get_type() == "shutdown":
            print("storalloc: closing the connection at the orchestrator's initiative")
            break

    time.sleep(1)

    sock.close(linger=0)
    context.term()
