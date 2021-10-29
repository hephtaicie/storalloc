""" Storalloc
    Default server
"""

import time
import uuid
import os
import sys
import zmq

# from storalloc.nvmet import nvme

from storalloc.resources import ResourceCatalog
from storalloc.config import config_from_yaml
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


class Server:
    """Default server for Storalloc"""

    def __init__(self, config_path: str, system_path: str, uid: str = None, verbose: bool = True):
        """Init a server using a yaml configuration file"""

        self.uid = uid or str(uuid.uuid4().hex)

        self.log = get_storalloc_logger(verbose)
        self.conf = config_from_yaml(config_path)
        self.rcatalog = ResourceCatalog(system_path)
        self.context, self.socket = self.zmq_init()

    def zmq_init(self):
        """Connect to orchestrator with ZeroMQ"""

        self.log.info(f"Initialise ZMQ Context for Server {self.uid}")
        context = zmq.Context()
        sock = context.socket(zmq.DEALER)  # pylint: disable=no-member

        if self.conf["transport"] == "tcp":
            url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['server_port']}"
        elif self.conf["transport"] == "ipc":
            url = f"ipc://{self.conf['orchestrator_be_ipc']}.ipc"

        self.log.debug(f"Server {self.uid} connecting to the orchestrator at ({url})")
        sock.connect(url)

        return (context, sock)

    def run(self, reset: bool, simulate: bool):
        """Server main loop
        Connect to the orchestrator, register (send available resources)
        and wait for allocation requests.
        """

        if not simulate and os.getuid() != 0:
            self.log.error("This script must be run with root privileges in a non-simulated mode!")
            # TODO : raise instead of exiting
            sys.exit(1)

        if reset:
            pass
        #   reset_resources(storage_resources)

        message = Message("register", self.rcatalog.storage_resources)
        self.socket.send(message.pack())

        while True:
            frames = self.socket.recv_multipart()
            client_id, data = frames[0], frames[1:]
            message = Message.from_packed_message(data)

            if message.type == "allocate":
                self.log.info(f"New allocation received [{message.content}]")
                job_id = message.content["job_id"]
                connection = {
                    "job_id": job_id,
                    "type": "nvme",
                    "nqn": "nqn.2014-08.com.vendor:nvme:nvm-subsystem-sn-d78432",
                }
                message = Message("connection", connection)
                self.socket.send_multipart([client_id, message.pack()])
            elif message.type == "deallocate":
                self.log.info(f"New deallocation received [{message.content}]")
            elif message.get_type() == "error":
                self.log.error(f"Error received [{message.content}]")
                break
            elif message.type == "shutdown":
                self.log.warning("The orchestrator has asked to close the connection")
                break

        time.sleep(1)

        self.socket.close(linger=0)
        self.context.term()
