#!/usr/bin/env python3

from src.job import Job

class JobQueue (object):

    def __init__ (self):
        super().__init__()

        self._queue = list ()

    def __iter__ (self):
        return JobQueueIterator (self)

    def add (self, job):
        self._queue.append (job)

    def remove (self, job):
        self._queue.remove (job)

    def count (self):
        return len (self._queue)

    def is_id_in_queue (self, job_id):
        for job in self._queue:
            if job.id() == job_id:
                return True
        return False


class JobQueueIterator (object):

    def __init__ (self, job_queue):
        self._job_queue = job_queue
        self._idx = 0

    def __next__(self):
        if self._idx < self._job_queue.count():
            job = self._job_queue._queue[self._idx]
            self._idx += 1
            return job
        else:
            raise StopIteration
