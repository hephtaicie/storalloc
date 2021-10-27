""" Storalloc
    Default Job implementation
"""

import datetime as dt

from storalloc.logging import get_storalloc_logger

# TODO : use enum class for job status ?


class Job:
    """Job

    A job object represents a request from a client, including connection id of the client,
    request details,  timings and the job status
    """

    def __init__(self, job_id, client_identity, request, simulate):
        """Init job with an id, a client, the request from whichit originates, and choose
        if it's part of a simulation or not
        """

        self.log = get_storalloc_logger()

        self.uid = job_id
        self.client_identity = client_identity

        self.request = request
        self.log.debug(f"[{self.uid:05}] New incoming request: {request}")

        self._status = "new"
        self._submission_time = dt.datetime.now(dt.timezone.utc)

        if self.request.start_time is not None and simulate:
            self.start_time = self.request.start_time
        else:
            self.start_time = dt.datetime.now(dt.timezone.utc)

        self.end_time = self.start_time + dt.timedelta(seconds=self.request.duration)

    def set_queued(self):
        """Change job status to queued"""
        self._status = "queued"

    def set_allocated(self):
        """Change job status to allocated"""
        self._status = "allocated"

    def set_pending(self):
        """Change job status to pending"""
        self._status = "pending"

    def is_pending(self):
        """Check whether or not job is currently in 'pending' state"""
        if self._status == "pending":
            return True
        return False

    def relative_start_time(self, origin):
        """Get job relative start time (compared to 'origin')"""
        return (self.start_time - origin).total_seconds()
