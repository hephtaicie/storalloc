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
