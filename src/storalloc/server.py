""" Storalloc
    Default server
"""

import uuid
import os
import zmq

# from storalloc.nvmet import nvme

from storalloc.request import RequestSchema, ReqState, StorageRequest
from storalloc.resources import ResourceCatalog
from storalloc.utils.config import config_from_yaml
from storalloc.utils.message import Message, MsgCat
from storalloc.utils.transport import Transport
from storalloc.utils.logging import get_storalloc_logger, add_remote_handler

# def reset_resources(storage_resources):
#    """Reset storage configurations
#
#    TODO: Move to a Storage class
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
    """Default storage server agent for Storalloc"""

    def __init__(
        self,
        config_path: str,
        system_path: str,
        uid: str = None,
        simulate: bool = True,
        verbose: bool = True,
    ):
        """Init a server using a yaml configuration file"""

        self.uid = uid or f"S-{str(uuid.uuid4().hex)[:6]}"
        self.conf = config_from_yaml(config_path)
        self.log = get_storalloc_logger(verbose, stderr_log=True)

        if not simulate and os.getuid() != 0:
            self.log.error("This script must be run with root privileges in a non-simulated mode!")
            raise AttributeError(
                "This script must be run with root privileges in a non-simulated mode!"
            )

        self.transports = self.zmq_init()

        self.rcatalog = ResourceCatalog(self.uid, system_path)
        self.schema = RequestSchema()

        self.log.info(f"Server {self.uid} ready")

    def zmq_init(self, remote_logging: bool = True):
        """Connect to orchestrator with ZeroMQ"""

        context = zmq.Context()

        # Logging PUBLISHER and associated handler ######################################
        if remote_logging:
            add_remote_handler(
                self.log,
                self.uid,
                context,
                f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_port']}",
                f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_sync_port']}",
            )

        self.log.info(f"Creating a DEALER socket for server {self.uid}")

        socket = context.socket(zmq.DEALER)  # pylint: disable=no-member
        socket.setsockopt_string(zmq.IDENTITY, self.uid)  # pylint: disable=no-member

        if self.conf["transport"] == "tcp":
            url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['server_port']}"
        elif self.conf["transport"] == "ipc":
            url = f"ipc://{self.conf['orchestrator_be_ipc']}.ipc"

        self.log.debug(f"Connecting DEALER socket [{self.uid}] to orchestrator at ({url})")
        socket.connect(url)

        return {"orchestrator": Transport(socket), "context": context}

    def allocate_storage(self, request: StorageRequest):
        """Allocate storage space on disks and update request with connection details"""

        # Only dummy values so far, the server never actually allocate storage
        request.nqn = "nqn.2014-08.com.vendor:nvme:nvm-subsystem-sn-d78432"
        request.alloc_type = "nvme"
        request.state = ReqState.ALLOCATED
        self.transports["orchestrator"].send_multipart(
            Message(MsgCat.REQUEST, self.schema.dump(request))
        )

    def deallocate_storage(self, request: StorageRequest):
        """Free space previously allocated to given request"""

    def run(self, reset: bool):
        """Server main loop
        Connect to the orchestrator, register (send available resources)
        and wait for allocation requests.
        """

        if reset:
            pass
        #   reset_resources(storage_resources)

        message = Message(
            MsgCat.REGISTRATION,
            [node.to_dict() for node in self.rcatalog.storage_resources[self.uid]],
        )
        self.log.debug(
            f"Sending registration message for server {self.uid}"
            + f" with {self.rcatalog.node_count()}"
        )
        self.transports["orchestrator"].send_multipart(message)

        allocated = 0
        deallocated = 0

        while True:

            identities, message = self.transports["orchestrator"].recv_multipart()

            if message.category == MsgCat.REQUEST:
                self.log.info(f"New request received from orchestrator {','.join(identities)}")
                request = self.schema.load(message.content)

                if request.state == ReqState.GRANTED:
                    self.log.info("New request in GRANTED state, processing now...")
                    self.allocate_storage(request)
                    allocated += 1
                elif request.state == ReqState.ENDED:
                    self.log.info("New request in ENDED state, processing now...")
                    self.deallocate_storage(request)
                    deallocated += 1
                else:
                    self.log.warning(f"New request has undesired state {request.state} ; skipping.")
                    continue
            elif message.category == MsgCat.NOTIFICATION:
                self.log.info(f"Notification received [{message.content}]")
            elif message.category == MsgCat.ERROR:
                self.log.error(f"Error received [{message.content}]")
                break
            elif message.category == MsgCat.SHUTDOWN:
                self.log.warning("The orchestrator has asked to close the connection")
                break

            if allocated % 1000 == 0 or deallocated % 1000 == 0:
                self.log.info(
                    f"# Quick stats : {allocated} req. allocated / {deallocated} req. deallocated"
                )

        self.transports["context"].destroy()
