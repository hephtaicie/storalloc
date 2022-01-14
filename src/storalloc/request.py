""" Storalloc
    Default storage allocation request
"""

import datetime
from dataclasses import dataclass
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

    capacity = fields.Float()
    duration = fields.TimeDelta()
    start_time = fields.DateTime()
    end_time = fields.DateTime()
    client_id = fields.Str()
    server_id = fields.Str()
    job_id = fields.Str()
    node_id = fields.Int()
    disk_id = fields.Int()
    alloc_type = fields.Str()
    nqn = fields.Str()
    state = EnumField(ReqState, by_value=True)
    reason = fields.Str()

    @post_load
    def make_request(self, data, **kwargs):  # pylint: disable=no-self-use
        """Deserialise into a StorageRequest object rather than a validated dict"""
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
    capacity: float
    duration: datetime.timedelta
    start_time: datetime.datetime
    end_time: datetime.datetime = None

    # Set for PENDING
    client_id: str = ""
    job_id: str = ""

    # Set for GRANTED
    node_id: int = 0
    disk_id: int = 0
    server_id: str = ""

    # Set for ALLOCATED
    alloc_type: str = ""
    nqn: str = ""

    # Always set
    state: ReqState = ReqState.OPENED

    # Set for FAILED or REFUSED
    reason: str = ""

    def __post_init__(self):
        """Add a few computed fields"""
        if self.end_time is None:
            self.end_time = self.start_time + self.duration

        if self.capacity <= 0:
            raise ValueError("Capacity must be strictly positive")

        if self.duration.total_seconds() <= 0:
            raise ValueError("Duration must be strictly positive")

    def __str__(self):
        """Representation of request depending on the current state"""

        desc = ""

        if self.state is ReqState.OPENED:
            desc = f"Request [OPENED] : {self.capacity} GB, {self.duration}, {self.start_time}"
        elif self.state is ReqState.PENDING:
            desc = f"Request [PENDING] : given job_id {self.job_id} / from client {self.client_id}"
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
        if (self.start_time + self.duration) > now:
            return False
        return True

    def overlaps(self, other) -> float:
        """Overlap time as a timedelta between this request and the one given as parameter"""

        # No overlap
        if other.start_time >= self.end_time or other.end_time <= self.start_time:
            return 0.0

        # Full overlap (self is in other)
        if other.start_time <= self.start_time and other.end_time >= self.end_time:
            return self.duration.total_seconds()
        # Full overlap (other is in self)
        if other.start_time >= self.start_time and other.end_time <= self.end_time:
            return other.duration.total_seconds()

        # Partial overlap
        if other.start_time > self.start_time and other.end_time > self.end_time:
            return (self.end_time - other.start_time).total_seconds()

        if other.start_time < self.start_time and other.end_time < self.end_time:
            return (other.end_time - self.start_time).total_seconds()

        raise ValueError  # Never expecting this to happen
