""" Storalloc
    Default Job Queue implementation
"""


class JobQueue:
    """A simple jobqueue"""

    def __init__(self):
        """Init Job Queue with simple list"""
        self._queue = []

    def __iter__(self):
        """Make JobQueue iterable"""
        return JobQueueIterator(self)

    def add(self, job):
        """Add job to job queue"""
        self._queue.append(job)

    def remove(self, job):
        """Removespecific job from job queue"""
        self._queue.remove(job)

    def count(self):
        """Return number of jobs currently in queue"""
        return len(self._queue)

    def is_id_in_queue(self, job_id):
        """Determine if given job_id matches one the jobs in queue"""
        for job in self._queue:
            if job.id() == job_id:
                return True
        return False

    def sort_asc_start_time(self):
        """Store jobs by start_time value"""
        self._queue.sort(key=lambda j: j.start_time())


class JobQueueIterator:
    """Iterator for job queue"""

    def __init__(self, job_queue):
        """Init iterator with a job_queue"""
        self._job_queue = job_queue
        self._idx = 0

    def __next__(self):
        """Iterate on job queue"""
        if self._idx < self._job_queue.count():
            job = self._job_queue._queue[self._idx]
            self._idx += 1
            return job
        raise StopIteration
