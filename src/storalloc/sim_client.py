""" Storalloc
    Simulation Client
"""

import datetime
from time import sleep
import uuid
from collections import deque

import yaml
import zmq

from storalloc.request import StorageRequest, RequestSchema
from storalloc.utils.message import Message, MsgCat
from storalloc.utils.transport import Transport
from storalloc.utils.config import config_from_yaml
from storalloc.utils.logging import get_storalloc_logger, add_remote_handler


class SimulationClient:
    """Default client for Storalloc"""

    def __init__(self, config_path: str, jobs_file: str, verbose: bool = True):
        """Init a client with a yaml configuration file"""

        self.uid = f"SC-{str(uuid.uuid4().hex)[:6]}"
        self.jobs_file = jobs_file
        self.log = get_storalloc_logger(verbose, stderr_log=True, logger_name="sim-storalloc")
        self.conf = config_from_yaml(config_path)
        self.transports = self.zmq_init()

        self.schema = RequestSchema()

        self.log.info(f"Simulation Client ID : {self.uid}")

    def __del__(self):
        """Close socket and terminate context upon exiting (should be done automatically in
        garbage collection, but it seems cleaner to manually make sure the socket is closed
        as soon as possible)"""
        self.transports["orchestrator"].socket.close(linger=0)
        self.transports["context"].term()

    def zmq_init(self, remote_logging: bool = True):
        """Prepare connection to the orchestrator and possibly add handler for remote logging to the application's main logger"""

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

        self.log.info(f"Creating DEALER socket for sim-client {self.uid}")
        socket = context.socket(zmq.DEALER)  # pylint: disable=no-member
        socket.setsockopt(zmq.IDENTITY, self.uid.encode("utf-8"))  # pylint: disable=no-member

        if self.conf["transport"] == "tcp":
            url = f"tcp://{self.conf['orchestrator_addr']}:{self.conf['client_port']}"
        elif self.conf["transport"] == "ipc":
            url = f"ipc://{self.conf['orchestrator_fr_ipc']}.ipc"

        self.log.debug(f"Connecting DEALER socket [{self.uid}] to orchestrator at ({url})")
        socket.connect(url)

        return {"orchestrator": Transport(socket), "context": context}

    def run(
        self,
    ):
        """Send a storage allocation request and await response"""

        stop = False
        discarded_no_write = 0
        errors = 0
        sent = 0
        processed = 0

        # Adapt to the "one file per month" output ?
        jobs = None
        with open(self.jobs_file, "r", encoding="utf-8") as jobs_stream:
            jobs = deque(yaml.load(jobs_stream, Loader=yaml.CSafeLoader)["jobs"])
            self.log.info(f"Retrieved {len(jobs)} jobs from file")

        while True:

            # If we received any answer from orchestrator, treat it first
            received = self.transports["orchestrator"].poll(10)
            if received:

                identities, message = self.transports["orchestrator"].recv_multipart()
                self.log.debug(f"Received message that transited from {';'.join(identities)}")
                if message.category == MsgCat.NOTIFICATION:
                    self.log.debug(f"New notification received [{message.content}]")
                elif message.category == MsgCat.ERROR:
                    self.log.error(f"Error from orchestrator : [{message.content}]")
                    errors += 1
                elif message.category == MsgCat.REQUEST:
                    request = self.schema.load(message.content)
                    processed += 1
                    self.log.info(
                        f"Request got back: {request} - ({processed} request related events processed)"
                    )
                else:
                    self.log.error("Unexpected message category, exiting client")
                    break

            # In any case, send more requests
            if not stop:
                try:
                    job = jobs.popleft()
                except IndexError:
                    self.send_eos()
                    stop = True
                    self.log.info("###### ALL REQUESTS SENT ######")
                    self.log.info(f"# - {sent} sent requests")
                    self.log.info(f"# - {processed} processed requests (at this point)")
                    self.log.info(f"# - {errors} errors received from orchestrator")
                    self.log.info(
                        f"# - {discarded_no_write} requests not sent because writtenBytes == 0 " \
                        " & readBytes == 0"
                    )
                    continue

                # Use either read or written bytes, depending on which has the higher
                # storage requirement (but we suppose the sum of the two is not reliable
                # due to potential overlap in the data read and written)
                capacity_requirement = max(job["writtenBytes"], job["readBytes"])

                if capacity_requirement > 0:
                    start_time = datetime.datetime.fromisoformat(job["startTime"])
                    end_time = datetime.datetime.fromisoformat(job["endTime"])

                    request = StorageRequest(
                        capacity=capacity_requirement / 1000000000,  # Convert to GB
                        duration=end_time - start_time,
                        start_time=start_time,
                    )
                    message = Message(MsgCat.REQUEST, self.schema.dump(request))
                    self.log.debug(f"Sending request for job : {job['id']}")
                    self.transports["orchestrator"].send_multipart(message)
                    sent += 1
                    sleep(0.01)
                else:
                    discarded_no_write += 1

        self.transports["context"].destroy()

    def send_eos(self):
        """Send End Of Simulation flag to orchestrator"""

        message = Message(MsgCat.EOS)
        self.transports["orchestrator"].send_multipart(message)
