""" Storalloc
    Default Client
"""

import datetime
import uuid
import zmq

from storalloc.request import StorageRequest, RequestSchema
from storalloc.utils.message import Message, MsgCat
from storalloc.utils.transport import Transport
from storalloc.utils.config import config_from_yaml
from storalloc.utils.logging import get_storalloc_logger


class Client:
    """Default client for Storalloc"""

    def __init__(self, config_path: str, uid: str = None, verbose: bool = True):
        """Init a client with a yaml configuration file"""

        self.log = get_storalloc_logger(verbose)
        self.conf = config_from_yaml(config_path)
        self.transports = self.zmq_init()

        self.uid = uid or f"C-{str(uuid.uuid4().hex)[:6]}"

        self.schema = RequestSchema()

    def __del__(self):
        """Close socket and terminate context upon exiting (should be done automatically in
        garbage collection, but it seems cleaner to manually make sure the socket is closed
        as soon as possible)"""
        self.transports["orchestrator"].socket.close(linger=0)
        self.transports["context"].term()

    def zmq_init(self, remote_logging: bool = True):
        """Prepare connection to the orchestrator and possibly add handler for
        remote logging to the application's main logger"""

        context = zmq.Context()

        # Logging PUBLISHER and associated handler ######################################
        if remote_logging:
            log_publisher = context.socket(zmq.PUB)  # pylint: disable=no-member
            log_publisher.bind(f"{self.conf['orchestrator_addr']}:{self.conf['log_server_port']}")
            self.log.addHandler(zmq.PUBHandler(log_publisher))  # pylint: disable=no-member

        self.log.info(f"Creating DEALER socket for client {self.uid}")
        socket = context.socket(zmq.DEALER)  # pylint: disable=no-member
        socket.setsockopt_string(zmq.IDENTITY, self.uid)  # pylint: disable=no-member

        if self.conf["transport"] == "tcp":
            url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['client_port']}"
        elif self.conf["transport"] == "ipc":
            url = f"ipc://{self.conf['orchestrator_fr_ipc']}.ipc"

        self.log.debug(f"Connecting DEALER socket [{self.uid}] to orchestrator at ({url})")
        socket.connect(url)

        return {"orchestrator": Transport(self.uid, socket), "context": context}

    def run(
        self,
        capacity: int,
        duration: datetime.timedelta,
        start_time: datetime.datetime = None,
    ):
        """Send a storage allocation request and await response"""

        request = StorageRequest(capacity=capacity, duration=duration, start_time=start_time)
        self.log.debug(f"New request from {self.uid} : {request}")
        message = Message(MsgCat.REQUEST, self.schema.dump(request))
        self.log.debug(f"Message : {message}")
        self.transports["orchestrator"].send_multipart(message)

        while True:

            identities, message = self.transports["orchestrator"].recv_multipart()
            self.log.debug(f"Received message that transited from {';'.join(identities)}")

            if message.category == MsgCat.NOTIFICATION:
                self.log.debug(f"New notification received [{message.content}]")
            elif message.category == MsgCat.ERROR:
                self.log.error("Error from orchestrator : [{message.content}]")
                break
            elif message.category == MsgCat.REQUEST:
                request = self.schema.load(message.content)
                self.log.debug(f"Request got back: {request}")
                # Do stuff with connection details...
                break
            else:
                self.log.error("Unexpected message category, exiting client")
                break

    def send_eos(self):
        """Send End Of Simulation flag to orchestrator"""

        message = Message(MsgCat.EOS)
        self.transports["orchestrator"].send_multipart(message)
