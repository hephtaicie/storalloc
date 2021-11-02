""" Storalloc
    Default Client
"""

import datetime
import uuid
import zmq

from storalloc.message import Message, MsgCat
from storalloc.config import config_from_yaml
from storalloc.logging import get_storalloc_logger


class Client:
    """Default client for Storalloc"""

    def __init__(self, config_path: str, uid: str = None, verbose: bool = True):
        """Init a client with a yaml configuration file"""

        self.uid = uid or str(uuid.uuid4().hex)

        self.log = get_storalloc_logger(verbose)
        self.conf = config_from_yaml(config_path)
        self.context, self.socket = self.zmq_init()

    def zmq_init(self):
        """Connect to orchestrator with ZeroMQ"""

        self.log.info(f"Initialise ZMQ Context for Client {self.uid}")
        context = zmq.Context()
        sock = context.socket(zmq.DEALER)  # pylint: disable=no-member

        if self.conf["transport"] == "tcp":
            url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['client_port']}"
        elif self.conf["transport"] == "ipc":
            url = f"ipc://{self.conf['orchestrator_fr_ipc']}.ipc"

        self.log.debug(f"Client {self.uid} connecting to the orchestrator at ({url})")
        sock.connect(url)

        return (context, sock)

    def run(
        self,
        size: int,
        time: datetime.datetime,
        start_time: datetime.datetime = None,
        eos: bool = False,
    ):
        """Init and start a new client"""

        request = f"{size},{time},{start_time}"
        self.log.info(f"Payload for new user request : [{request}]")

        if eos:
            message = Message(MsgCat.EOS, request)
        else:
            message = Message(MsgCat.REQUEST, request)

        self.log.debug(f"Submitting user message [{message}]")

        self.socket.send(message.pack())

        while True:
            data = self.socket.recv()
            message = Message.unpack(data)

            if message.category == MsgCat.NOTIFICATION:
                self.log.info(f"New notification received [{message.content}]")
            elif message.category == MsgCat.ALLOCATION:
                self.log.info(f"New allocation received [{message.content}]")
                # Do stuff with connection details
                break
            elif message.category == MsgCat.ERROR:
                break
            elif message.category == MsgCat.SHUTDOWN:
                self.log.warning("Orchestrator has asked to close the connection")
                break

        # should be part of some context manager ?
        self.socket.close(linger=0)
        self.context.term()
