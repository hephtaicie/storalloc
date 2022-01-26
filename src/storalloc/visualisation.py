"""Basic visualisation server using Bokeh"""


import threading
from functools import partial
import uuid
import datetime

import zmq

import bokeh
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, DatetimeTickFormatter
from bokeh.server.server import Server
from bokeh.transform import dodge
from bokeh.models import FactorRange

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
        self,
        config_path: str,
        uid: str = None,
        verbose: bool = True,
        context: zmq.Context = None,
    ):
        """Init zmq comm"""

        self.uid = uid or f"VIS-{str(uuid.uuid4().hex)[:6]}"
        self.conf = config_from_yaml(config_path)
        self.log = get_storalloc_logger(verbose)
        self.context = context if context else zmq.Context()
        self.max_points = self.conf["visualisation"]["max_points"]

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

    def zmq_init_subscriber(self, simulation: bool):
        """Init subscriber for either simulation or orchestration events"""
        # Visualisation SUBSCRIBER (receive events from simulation OR orchestrator) ################
        visualisation_socket = self.context.socket(zmq.SUB)  # pylint: disable=no-member
        visualisation_socket.setsockopt(zmq.SUBSCRIBE, b"sim")  # pylint: disable=no-member
        if simulation:
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

        # Sources
        sources = {}
        sources["alloc"] = ColumnDataSource(data={"time": [], "value": []})
        sources["calloc"] = ColumnDataSource(data={"time": [], "value": []})
        sources["calloc_disk"] = ColumnDataSource(data={"disk_label": [], "ca": [], "max_ca": []})

        # Plot Allocated GB - Simulation
        alloc_plot = figure(
            y_axis_label="Allocated GB",
            x_axis_label="Simulation Time",
            x_axis_type="datetime",
            title="Used GBs across all disks - Simulation",
            sizing_mode="stretch_both",
        )
        alloc_plot.xaxis[0].formatter = DatetimeTickFormatter()
        alloc_plot.step(
            x="time", y="value", source=sources["alloc"], line_width=1, color="darkgreen"
        )

        # Plot Concurrent allocations (vbar) - Simulation
        calloc_plot = figure(
            y_axis_label="Concurrent Allocations",
            x_axis_label="Simulation Time",
            x_axis_type="datetime",
            title="Number of concurrent allocations - Simulation",
            sizing_mode="stretch_both",
        )
        calloc_plot.xaxis[0].formatter = DatetimeTickFormatter()
        calloc_plot.dot(x="time", y="value", source=sources["calloc"], size=15, color="dodgerblue")

        ca_per_disk = figure(
            y_range=FactorRange(),
            x_range=(0, 5),
            title="Concurrent Allocations per disk",
            width=400,
            sizing_mode="stretch_height",
        )

        ca_per_disk.hbar(
            y=dodge("disk_label", -0.25, range=ca_per_disk.y_range),
            right="ca",
            source=sources["calloc_disk"],
            height=0.2,
            color="#c9d9d3",
            legend_label="CA",
        )

        ca_per_disk.hbar(
            y=dodge("disk_label", 0.25, range=ca_per_disk.y_range),
            right="max_ca",
            source=sources["calloc_disk"],
            height=0.2,
            color="#e84d60",
            legend_label="Max CA",
        )

        ca_per_disk.y_range.range_padding = 0.1
        ca_per_disk.legend.location = "top_left"
        ca_per_disk.legend.orientation = "horizontal"

        doc.add_root(
            bokeh.layouts.row(
                bokeh.layouts.column(alloc_plot, calloc_plot, sizing_mode="stretch_width"),
                ca_per_disk,
            )
        )

        async def update_plots(update_data):
            for plot_key, update in update_data.items():
                if plot_key == "calloc_disk":
                    ca_per_disk.y_range.factors = update["disk_label"]
                    ca_per_disk.x_range.end = max(update["max_ca"]) + 2
                    sources[plot_key].stream(update, len(update["disk_label"]))
                else:
                    sources[plot_key].stream(update, self.max_points)

        def blocking_task():

            vis_sub = self.zmq_init_subscriber(simulation=True)

            total_used_storage_sim = 0
            disks = {}
            disk_labels = []
            disk_max_ca = []

            while threading.main_thread().is_alive():

                # We need to use polling so that the thread
                # can't get stuck on recv_multipart when simulation
                # is over (otherwise it exits the while loop but still blocks)
                if vis_sub.poll(timeout=100) == 0:
                    continue

                topic, msg = vis_sub.recv_multipart()
                topic = topic[0]
                update = {}

                if topic != "sim":
                    print(f"INVALID TOPIC {topic}. How did this message reach the visualisation ?")
                    continue

                if msg.category is MsgCat.DATALIST:
                    updt_values = msg.content
                    for plot, xval, yval in updt_values:
                        if plot == "alloc":
                            total_used_storage_sim += yval
                            xval = datetime.datetime.fromtimestamp(xval)
                            update[plot] = {"time": [xval], "value": [total_used_storage_sim]}
                        elif plot == "calloc":
                            xval = datetime.datetime.fromtimestamp(xval)
                            update[plot] = {"time": [xval], "value": [yval]}
                        elif plot == "calloc_disk":
                            if xval not in disks:
                                disks[xval] = [yval, yval]
                                disk_labels = list(disks.keys())
                                disk_max_ca = [val[1] for val in disks.values()]
                            else:
                                if yval > disks[xval][1]:
                                    disks[xval] = [yval, yval]  # Update current and max
                                    disk_max_ca = [val[1] for val in disks.values()]
                                else:
                                    disks[xval][0] = yval  # update current only

                            disk_ca = [val[0] for val in disks.values()]
                            update[plot] = {
                                "disk_label": disk_labels,
                                "max_ca": disk_max_ca,
                                "ca": disk_ca,
                            }
                        else:
                            print(f"Unknown plot {plot}")

                    doc.add_next_tick_callback(partial(update_plots, update_data=update))

        thread = threading.Thread(target=blocking_task)
        thread.start()

    def run(self, simulation: bool = True):
        """Run simulation or orchestration visualisation"""

        if simulation:
            bkserver = Server({"/": self.simulation_vis}, num_procs=1)
        else:
            raise NotImplementedError("Visualisation can only be used with simulation so far.")

        try:
            bkserver.io_loop.add_callback(bkserver.show, "/")
            bkserver.start()
            bkserver.io_loop.start()
        except KeyboardInterrupt:
            self.log.info("Visualisation server terminated by keyboard interrupt")
