""" Storalloc
    Default storage allocation request
"""

import datetime
from dataclasses import dataclass, field
from enum import Enum

from marshmallow import Schema, fields
from marshmallow_enum import EnumField


class ReqState(Enum):
    """Request State Enum"""

    OPENED = 1
    PENDING = 2
    GRANTED = 3
    REFUSED = 4
    ALLOCATED = 5
    FAILED = 6
    ENDED = 7


class RequestSchema(Schema):
    """Marshmallow schema for Request class"""

    capacity = fields.Int()
    duration = fields.TimeDelta()
    start_time = fields.DateTime()
    job_id = fields.Str()
    node_id = fields.Str()
    disk_id = fields.Str()
    alloc_type = fields.Str()
    nqn = fields.Str()
    state = EnumField(ReqState, by_value=True)
    reason = fields.Str()


@dataclass
class StorageRequest:
    """Storage request object

    Represents a request for storage allocation, as emited by
    a client, and which can be passed around between the different
    components of Storalloc, each time having its state updated.

    TODO: add setter in order to recompute end_time if either duration
    or start_time are amended
    """

    # Set for OPENED
    capacity: int = 0
    duration: datetime.timedelta = None
    start_time: datetime.datetime = None

    # Set for PENDING
    job_id: str = ""

    # Set for GRANTED
    node_id: str = ""
    disk_id: str = ""

    # Set for ALLOCATED
    alloc_type: str = ""
    nqn: str = ""

    # Always set
    state: ReqState = ReqState.OPENED

    # Set for FAILED or REFUSED
    reason: str = ""

    def __post_init__(self):
        """Add a few computed fields"""
        self.end_time = self.start_time + self.duration

    def __str__(self):
        """Representation of request depending on the current state"""

        desc = ""

        if self.state == ReqState.OPENED:
            desc = f"Request [OPENED] : {self.capacity} GB, {self.duration}, {self.start_time}"
        if self.state == ReqState.PENDING:
            desc = f"Request [PENDING] : {self.capacity} GB, {self.duration}, {self.start_time}"
        elif self.state == ReqState.GRANTED:
            desc = (
                f"Request [GRANTED] : {self.capacity} GB, for {self.duration}, "
                + f"{self.start_time} on {self.node_id}:{self.disk_id}"
            )
        elif self.state == ReqState.REFUSED:
            desc = "Request [REFUSED] by orchestrator"
        elif self.state == ReqState.ALLOCATED:
            desc = (
                f"Request [ALLOCATED] by {self.node_id} on disk "
                + f"{self.disk_id} for {self.capacity} GB - "
                + f"Connection detail {self.nqn}, {self.alloc_type}"
            )
        elif self.state == ReqState.FAILED:
            desc = f"Request [FAILED] on {self.node_id}, reason : {self.reason}"
        elif self.state == ReqState.ENDED:
            desc = f"Request [ENDED] at {self.end_time} ({self.node_id}"
        else:
            desc = "[ERROR] Somehow the current state of this request is unknown"

        return desc

    def is_overdue(self, current_time: datetime.datetime = None):
        """Check if the current request should be deallocated based
        on its start_time and duration"""

        now = current_time if current_time else datetime.datetime.now()
        if now < (self.start_time + self.duration):
            return True
        return False

    def __eq__(self, other):
        """If end_time for both request are equal, we consider the request to be equal
        (those operators are used for ordering request in queue)"""

        if self.end_time == other.end_time:
            return True
        return False

    def __gt__(self, other):
        """Compare two request by end_time (A > B means A's storage allocation
        will be over after B's storage allocation)"""
        return self.end_time > other.end_time

    def __lt__(self, other):
        return not self > other

    def __ge__(self, other):
        return self.end_time >= other.end_time

    def __le__(self, other):
        return not self >= other


@dataclass
class Request:
    """Default storage allocation request"""

    raw_request: str = field(repr=False, compare=False)
    capacity: int = field(init=False)
    duration: int = field(init=False)
    start_time: "typing.Any" = field(init=False)

    def __post_init__(self):
        """Init request object from raw request"""

        req_parts = self.raw_request.split(",")
        assert len(req_parts) == 3  # capacity, duration, start_time
        self.capacity = int(req_parts[0])
        self.duration = int(req_parts[1])

        if self.capacity <= 0 or self.duration <= 0:
            raise ValueError("Capacity or duration is <= 0 for request")

        if req_parts[2] != "None":
            self.start_time = datetime.datetime.strptime(req_parts[2], "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = None

    def __str__(self):
        return f"{self.capacity} GB, {datetime.timedelta(seconds=self.duration)}, {self.start_time}"
