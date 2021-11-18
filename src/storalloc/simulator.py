""" Storalloc Simulation module
    Handler for simulation event and Simpy wrapper.
"""

import simpy


class Simulator:
    """basic simulator"""

    def __init__(self):
        """Init simulation with new simpy env"""

        self.env = simpy.Environment()

    def simulate_scheduling(self, job, earliest_start_time):
        """Simpy"""

        yield self.env.timeout(job.sim_start_time(earliest_start_time))

        target_node, target_disk = self.scheduling_strategy.compute(self.rcatalog, job)

        # If a disk on a node has been found, we allocate the request
        if target_node >= 0 and target_disk >= 0:
            self.grant_allocation(job, target_node, target_disk)
        else:
            self.log.warning(f"Job<{job.uid:05}> - Unable to allocate request. Exiting...")
            sys.exit(1)

        # Duration + Fix seconds VS minutes
        yield self.env.timeout(job.sim_start_time())





    def process_queue(self, simulate: bool):
        """Process request queue"""

        if simulate:
            if end_of_simulation:

                earliest_start_time = datetime.datetime.now()
                latest_end_time = datetime.datetime(1970, 1, 1)

                self.pending_jobs.sort_asc_start_time()

                for job in self.pending_jobs:
                    if job.start_time() < earliest_start_time:
                        earliest_start_time = job.start_time()
                    if job.end_time() > latest_end_time:
                        latest_end_time = job.end_time()

                sim_duration = (latest_end_time - earliest_start_time).total_seconds() + 1

                for job in self.pending_jobs:
                    self.env.process(self.simulate_scheduling)

                self.env.run(until=sim_duration)
        else:
            for job in self.pending_jobs:
                target_node, target_disk = self.scheduling_strategy.compute(self.rcatalog, job)

                # If a disk on a node has been found, we allocate the request
                if target_node >= 0 and target_disk >= 0:
                    self.grant_allocation(job, target_node, target_disk)
                else:
                    if not job.is_pending():
                        self.log.debug(
                            f"Job<{job.uid:05}> - Currently unable to allocate incoming request"
                        )
                        job.status = JobStatus.PENDING


