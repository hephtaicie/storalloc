""" Storalloc
    Default server
"""

import time
import os
import sys
import zmq

# from storalloc.nvmet import nvme

from storalloc.resources import ResourceCatalog
from storalloc.config_file import ConfigFile
from storalloc.message import Message
from storalloc.logging import get_storalloc_logger

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
    sock = context.socket(zmq.DEALER)  # pylint: disable=no-member
    sock.connect(url)

    return (context, sock)


def run(config_file, system, reset, simulate):
    """Server main loop
    Connect to the orchestrator, register (send available resources)
    and wait for allocation requests.
    """

    log = get_storalloc_logger()

    if not simulate and os.getuid() != 0:
        log.error("This script must be run with root privileges in a non-simulated mode!")
        sys.exit(1)

    conf = ConfigFile(config_file)

    resource_catalog = ResourceCatalog(system)

    orchestrator_url = f"tcp://{conf.get_orch_ipv4()}:{conf.get_orch_port()}"
    context, sock = zmq_init(orchestrator_url)

    if reset:
        pass
    #   reset_resources(storage_resources)

    log.debug(f"Registering to the orchestrator ({orchestrator_url})")
    message = Message("register", resource_catalog.storage_resources)
    sock.send(message.pack())

    while True:
        client_id, data = sock.recv_multipart()
        message = Message.from_packed_message(data)

        if message.type == "allocate":
            log.info(f"New allocation received [{message.content}]")
            job_id = message.content["job_id"]
            connection = {
                "job_id": job_id,
                "type": "nvme",
                "nqn": "nqn.2014-08.com.vendor:nvme:nvm-subsystem-sn-d78432",
            }
            message = Message("connection", connection)
            sock.send_multipart([client_id, message.pack()])
        elif message.type == "deallocate":
            log.info(f"New deallocation received [{message.content}]")
        elif message.get_type() == "error":
            log.error(f"Error received [{message.content}]")
            break
        elif message.type == "shutdown":
            log.warning("The orchestrator has asked to close the connection")
            break

    time.sleep(1)

    sock.close(linger=0)
    context.term()
