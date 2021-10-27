""" Storalloc
    ThreadAllocation
"""

import threading
import datetime
import subprocess

time_lock = threading.Lock()


class ThreadAlloc_meta(type):
    """ThreadAllocation metaclass"""

    def __init__(cls, *args, **kwargs):

        cls._current_time = None

    @property
    def time(cls):
        return cls._current_time

    @time.setter
    def time(cls, value):
        cls._current_time = value
        for instance in ThreadAlloc.instances:
            instance.check_deallocation()


class ThreadAlloc(threading.Thread, metaclass=ThreadAlloc_meta):

    instances = []

    def __init__(self, name, alloc_id, alloc_duration, alloc_capacity, alloc_timestamp, target):

        ThreadAlloc.instances.append(self)
        self._lock_allocation = threading.Lock()
        self._lock_allocation.acquire()

        super(ThreadAlloc, self).__init__()
        self._name = name
        self._alloc_id = alloc_id
        self._alloc_duration = alloc_duration
        self._target = target
        self._alloc_capacity = alloc_capacity
        self._alloc_timestamp = datetime.datetime.strptime(alloc_timestamp, "%Y-%m-%d %H:%M:%S")

        self._alloc_running = True

        time_lock.acquire()

        if ThreadAlloc.time is not None:
            if ThreadAlloc.time < self._alloc_timestamp:
                ThreadAlloc.time = self._alloc_timestamp
        else:
            ThreadAlloc.time = self._alloc_timestamp

        timeLock.release()

    @classmethod
    def increaseCurrentTime(self, duration):
        timeLock.acquire()
        print("ThreadAlloc : current time = " + str(ThreadAlloc.time))
        print("ThreadAlloc : increase currentTime by " + str(duration) + " minutes")
        ThreadAlloc.time += datetime.timedelta(minutes=duration)
        print("ThreadAlloc : current time = " + str(ThreadAlloc.time))
        timeLock.release()

    def checkDeallocation(self):
        print("Checking if Thread " + str(self._allocID) + " resources have to be deallocated")
        if ThreadAlloc.time >= self._allocTimestamp + datetime.timedelta(
            minutes=self._allocDuration
        ):
            print(
                "Thread "
                + str(self._allocID)
                + " resources have to be deallocated, releasing the lock -> back to run()"
            )
            if not self._lockAllocation.locked():
                self._lockAllocation.acquire()
            self._lockAllocation.release()

    def run(self):

        print("Thread " + str(self._allocID) + " started sucessfully")
        print(
            "Thread "
            + str(self._allocID)
            + " will deallocate the resources when the allocation duration is over"
        )

        self._lockAllocation.acquire()
        print(
            "Thread "
            + str(self._allocID)
            + " resources will be released, submitting request to the client ..."
        )
        ThreadAlloc.instances.remove(self)
        deallocationClientCall = (
            "./client.py -c config/client/config.yml -s "
            + str(self._allocCapacity)
            + " -t "
            + str(self._allocDuration)
            + " -v -d "
            + str(self._allocID)
            + ' -timestamp "'
            + str(ThreadAlloc.time)
            + '"'
        )
        subprocess.run(deallocationClientCall, shell=True, check=True)

        print("Thread " + str(self._allocID) + " has released its resources !")
