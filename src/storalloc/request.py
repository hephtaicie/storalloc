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
    GRANTED = 2
    REFUSED = 3
    ALLOCATED = 4
    FAILED = 5
    READY = 6


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
    """

    capacity: int = 0
    duration: datetime.timedelta = None
    start_time: datetime.datetime = None
    job_id: str = ""
    node_id: str = ""
    disk_id: str = ""
    alloc_type: str = ""
    nqn: str = ""
    state: ReqState = ReqState.OPENED
    reason: str = ""

    def __str__(self):
        """Representation of request depending on the current state"""

        if self.state == ReqState.OPENED:
            return f"Request [OPENED] : {self.capacity} GB, {self.duration}, {self.start_time}"
        elif self.state == ReqState.GRANTED:
            return (
                f"Request [GRANTED] : {self.capacity} GB, for {self.duration}, "
                + f"{self.start_time} on {self.node_id}:{self.disk_id}"
            )
        elif self.state == ReqState.REFUSED:
            return f"Request [REFUSED] by orchestrator"
        elif self.state == ReqState.ALLOCATED:
            return f"Request [ALLOCATED] by {self.node_id} on disk {self.disk_id} for {self.capacity} GB"
        elif self.state == ReqState.FAILED:
            return f"Request [FAILED] on {self.node_id}, reason : {self.reason}"
        elif self.state == ReqState.READY:
            return f"Request [READY] with connection parameters : {self.node_id}, {self.nqn}, {self.alloc_type}, {self.capacity}"
        else:
            return f"[ERROR] Somehow the current state of this request is unknown"


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
