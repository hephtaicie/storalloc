"""Basic visualisation server using Bokeh"""


import threading
from functools import partial
import uuid

import zmq

import bokeh
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.server.server import Server

from storalloc.utils.logging import get_storalloc_logger, add_remote_handler
from storalloc.utils.config import config_from_yaml
from storalloc.utils.message import MsgCat
from storalloc.utils.transport import Transport


class Visualisation:
    """Visualisation server based on a Bokeh app, and able
    to receive and display events either from simulation server or
    orchestrator
    """

    def __init__(
        self, config_path: str, uid: str = None, simulation: bool = True, verbose: bool = True
    ):
        """Init zmq comm"""

        self.uid = uid or f"SIM-{str(uuid.uuid4().hex)[:6]}"
        self.simulation = simulation
        self.conf = config_from_yaml(config_path)
        self.log = get_storalloc_logger(verbose)
        self.context = zmq.Context()

        # Don't ever use self.logger inside a bokeh thread, as this would mean
        # using the same zmq Socket from differents threads, which has und. behaviour
        if verbose:
            add_remote_handler(
                self.log,
                self.uid,
                self.context,
                f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_port']}",
                f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_sync_port']}",
            )

    def zmq_init_subscriber(self):
        """Init subscriber for either simulation or orchestration events"""

        # Visualisation SUBSCRIBER (receive events from simulation OR orchestrator) ################
        visualisation_socket = self.context.socket(zmq.SUB)  # pylint: disable=no-member
        visualisation_socket.setsockopt(zmq.SUBSCRIBE, b"sim")  # pylint: disable=no-member
        if self.simulation:
            self.log.info("Connecting visualisation socket to simulation server...")
            visualisation_socket.connect(
                f"tcp://{self.conf['simulation_addr']}:{self.conf['s_visualisation_port']}"
            )
        else:
            self.log.info("Connecting visualisation socket to orchestrator...")
            visualisation_socket.connect(
                f"tcp://{self.conf['orchestrator_addr']}:{self.conf['o_visualisation_port']}"
            )

        return Transport(visualisation_socket)

    def simulation_vis(self, doc):
        """Start a Bokeh app for simulation events"""

        # Common
        tick = [16 * n for n in range(128)]
        y_axis_range = bokeh.models.DataRange1d(start=0)

        # Sources
        sources = {}
        sources["alloc"] = ColumnDataSource(data={"time": [], "value": []})

        # Plot - Simulation
        sim_plot = figure(
            y_axis_label="Allocated GB",
            x_axis_label="Simulation Time",
            title="Used GBs across all disks - Simulation",
            y_range=y_axis_range,
            sizing_mode="stretch_both",
        )
        sim_plot.ygrid.ticker = tick
        sim_plot.vbar(x="time", y="value", source=sources["alloc"], line_width=1, color="darkgreen")

        # doc.add_root(bokeh.layouts.column(sim_plot, sizing_mode="stretch_both"))
        doc.add_root(sim_plot)

        async def update_sim(time, value):
            sources["alloc"].stream(dict(time=[time], value=[value]))

        def blocking_task():

            vis_sub = self.zmq_init_subscriber()

            total_used_storage_sim = 0
            total_capa = 0

            while threading.main_thread().is_alive():

                # We need to use polling so that the thread
                # can't get stuck on recv_multipart when simulation
                # is over (otherwise it exits the while loop but still blocks)
                if vis_sub.poll(timeout=100) == 0:
                    continue

                topic, msg = vis_sub.recv_multipart()
                topic = topic[0]

                if topic == "sim":
                    if msg.category is MsgCat.DATAPOINT and msg.content[0] == "alloc":
                        print(f"New simulation point : {msg.content}")
                        total_used_storage_sim += msg.content[2]
                        doc.add_next_tick_callback(
                            partial(update_sim, time=msg.content[1], value=total_used_storage_sim)
                        )

                else:
                    print(f"INVALID TOPIC {topic}. How did this message reach the visualisation ?")

        thread = threading.Thread(target=blocking_task)
        thread.start()

    def run(self):
        """Run simulation or orchestration visualisation"""

        if self.simulation:
            bkserver = Server({"/": self.simulation_vis}, num_procs=1)
        else:
            raise NotImplementedError("Visualisation can only be used with simulation so far.")

        bkserver.io_loop.add_callback(bkserver.show, "/")
        bkserver.start()
        bkserver.io_loop.start()
