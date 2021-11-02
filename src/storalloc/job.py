""" Storalloc
    Default Job implementation
"""

import datetime as dt
from enum import Enum
from dataclasses import dataclass

from storalloc.request import Request
from storalloc.logging import get_storalloc_logger


class JobStatus(Enum):
    """Enum class representing the current status of a job"""

    NEW = 1
    QUEUED = 2
    ALLOCATED = 3
    PENDING = 4


@dataclass
class Job:  # pylint: disable=too-many-instance-attributes
    """Job

    A job object represents a request from a client, including connection id of the client,
    request details,  timings and the job status
    """

    uid: int
    client_identity: int
    request: Request
    simulate: bool
    status: JobStatus = JobStatus.NEW

    def __post_init__(self):
        """Init job with an id, a client, the request from whichit originates, and choose
        if it's part of a simulation or not
        """

        self.log = get_storalloc_logger()
        self.log.debug(f"Job<{self.uid:05}> - New incoming request [{self.request}]")

        self._submission_time = dt.datetime.now(dt.timezone.utc)

        if self.request.start_time is not None and self.simulate:
            self.start_time = self.request.start_time
        else:
            self.start_time = dt.datetime.now(dt.timezone.utc)

        self.end_time = self.start_time + dt.timedelta(seconds=self.request.duration)

    def is_pending(self):
        """Check whether or not job is currently in 'pending' state"""
        return self.status == JobStatus.PENDING

    def relative_start_time(self, origin):
        """Get job relative start time (compared to 'origin')"""
        return (self.start_time - origin).total_seconds()
