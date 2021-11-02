""" Storalloc
    Default storage allocation request
"""

import datetime as dt
from dataclasses import dataclass, field


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
            self.start_time = dt.datetime.strptime(req_parts[2], "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = None

    def __str__(self):
        return f"{self.capacity} GB, {dt.timedelta(seconds=self.duration)}, {self.start_time}"
