#!/usr/bin/env python3

import datetime as dt
import logging
from storalloc.request import Request


class Job(object):
    """Job

    A job object represents a request from a client, including connection id of the client,
    request details,  timings and the job status
    """

    def __init__(self, job_id, client_identity, request, simulate):
        super().__init__()

        self._job_id = job_id
        self._client_identity = client_identity

        self.request = request
        logging.debug(
            "[" + str(self._job_id).zfill(5) + "] New incoming request: " + self.request.to_string()
        )

        self._status = "new"
        self._submission_time = dt.datetime.now(dt.timezone.utc)

        if self.request.start_time() is not None and simulate:
            self._start_time = self.request.start_time()
        else:
            self._start_time = dt.datetime.now(dt.timezone.utc)

        self._end_time = self._start_time + dt.timedelta(seconds=self.request.duration())

    def id(self):
        return self._job_id

    def client_identity(self):
        return self._client_identity

    def start_time(self):
        return self._start_time

    def end_time(self):
        return self._end_time

    def set_queued(self):
        self._status = "queued"

    def set_allocated(self):
        self._status = "allocated"

    def set_pending(self):
        self._status = "pending"

    def is_pending(self):
        if self._status == "pending":
            return True
        return False

    def relative_start_time(self, origin):
        return (self._start_time - origin).total_seconds()
