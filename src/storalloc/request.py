""" Storalloc
    Default storage allocation request
"""

import datetime
from dataclasses import dataclass, field
from enum import Enum

from marshmallow import Schema, fields, post_load
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

    @post_load
    def make_request(self, data, **kwargs):
        return StorageRequest(**data)


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

        if self.state is ReqState.OPENED:
            desc = f"Request [OPENED] : {self.capacity} GB, {self.duration}, {self.start_time}"
        elif self.state is ReqState.PENDING:
            desc = f"Request [PENDING] : {self.capacity} GB, {self.duration}, {self.start_time}"
        elif self.state is ReqState.GRANTED:
            desc = (
                f"Request [GRANTED] : {self.capacity} GB, for {self.duration}, "
                + f"{self.start_time} on {self.node_id}:{self.disk_id}"
            )
        elif self.state is ReqState.REFUSED:
            desc = "Request [REFUSED] by orchestrator"
        elif self.state is ReqState.ALLOCATED:
            desc = (
                f"Request [ALLOCATED] by {self.node_id} on disk "
                + f"{self.disk_id} for {self.capacity} GB - "
                + f"Connection detail {self.nqn}, {self.alloc_type}"
            )
        elif self.state is ReqState.FAILED:
            desc = f"Request [FAILED] on {self.node_id}, reason : {self.reason}"
        elif self.state is ReqState.ENDED:
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
