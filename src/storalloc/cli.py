""" Storalloc
    CLI entrypoint

    No loggers are defined here, we'll only use click.secho()
    for basic text output. Processing and logging has to be
    'contained' in other parts of the app.
"""

import datetime
import click

from storalloc import client, server, log_server, simulation, visualisation, sim_client
from storalloc.orchestrator import router


@click.group()
@click.pass_context
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose mode")
def cli(ctx, verbose):
    """Storalloc CLI"""
    click.secho("## Storalloc ##", fg="cyan")
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


# STORAGE SERVER
@cli.command("server")
@click.pass_context
@click.option(
    "-c",
    "--config",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the StorAlloc configuration file",
)
@click.option(
    "-s",
    "--system",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path of the storage system description (YAML)",
)
@click.option(
    "-r",
    "--reset",
    is_flag=True,
    help="Reset the existing storage configurations",
)
@click.option(
    "--simulate",
    is_flag=True,
    help="Receive requests only. No actual storage allocation",
)
def run_server(ctx, config, system, reset, simulate):
    """Server command"""
    click.secho("[~] Starting server...", fg="white", bg="cyan")
    cli_server = server.Server(config, system, simulate, verbose=ctx.obj["verbose"])
    cli_server.run(reset)


# ORCHESTRATOR
@cli.command("orchestrator")
@click.pass_context
@click.option(
    "-c",
    "--config",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the StorAlloc configuration file",
)
def run_orchestrator(ctx, config):
    """Orchestrator command"""
    click.secho("[~] Starting orchestrator...", fg="yellow")
    orchestrator = router.Router(config, ctx.obj["verbose"])
    orchestrator.run()


# CLIENT
@cli.command("client")
@click.pass_context
@click.option(
    "-c",
    "--config",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the StorAlloc configuration file",
)
@click.option(
    "-s",
    "--size",
    required=True,
    type=click.IntRange(min=0, min_open=True),
    help="Size of the requested storage allocation (GB)",
)
@click.option(
    "-t",
    "--time",
    required=True,
    type=click.DateTime(formats=["%H:%M:%S"]),
    help="Total run time of the storage allocation",
)
@click.option(
    "--start-time",
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    default=None,
    help="Timestamp of the allocation's starting time (Simulation only)",
)
@click.option(
    "-e",
    "--eos",
    is_flag=True,
    help="Send EndOfSimulation flag to the orchestrator (Simulation only)",
)
def run_client(ctx, config, size, time, start_time, eos):
    """Start a Storalloc client (an orchestrator need to be already running)"""

    click.secho("[~] Starting client...", fg="cyan")
    # Convert duration of requested storage allocation to seconds
    time_delta = datetime.timedelta(hours=time.hour, minutes=time.minute, seconds=time.second)
    if start_time is None:
        start_time = datetime.datetime.now()

    client_endpoint = client.Client(config, verbose=ctx.obj["verbose"])
    if not eos:
        client_endpoint.run(size, time_delta, start_time)
    else:
        click.secho("[!] Not implemented", fg="red")


# SIMULATION CLIENT
@cli.command("sim-client")
@click.pass_context
@click.option(
    "-c",
    "--config",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the StorAlloc configuration file",
)
@click.option(
    "-j",
    "--jobs",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to a Yaml file containing the list of jos to simulate",
)
def run_sim_client(ctx, config, jobs):
    """Run a simulation client, using a predetermined list of jobs"""

    click.secho("[~] Starting SIMULATION client...", fg="cyan")
    client_endpoint = sim_client.SimulationClient(config, jobs, verbose=ctx.obj["verbose"])
    client_endpoint.run()


# LOG-SERVER
@cli.command("log-server")
@click.pass_context
@click.option(
    "-c",
    "--config",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the StorAlloc configuration file",
)
def logging(ctx, config):
    """Start a StorAlloc log server, which collects and display logs from the other components."""

    click.secho("[~] Starting log-server.", fg="green")
    logs = log_server.LogServer(config, verbose=ctx.obj["verbose"])
    logs.run()


# Simulation
@cli.command("sim-server")
@click.pass_context
@click.option(
    "-c",
    "--config",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the StorAlloc configuration file",
)
@click.option(
    "-r",
    "--real-time",
    required=False,
    type=float,
    default=1.0,
    help="Real Time factor. If < 1, will make the simulation server use"
    + "simpy.rt.RealtimeEnvironment, with given factor. Default is 1.",
)
def run_sim(ctx, config, real_time):
    """Start a StorAlloc simulation serer, based on Simpy"""

    click.secho("[~] Starting simulation-server.", fg="green")
    sim = simulation.Simulation(config, verbose=ctx.obj["verbose"], rt_factor=real_time)
    sim.run()


# Visualisation
@cli.command("visualisation")
@click.pass_context
@click.option(
    "-c",
    "--config",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the StorAlloc configuration file",
)
@click.option(
    "-s",
    "--sim",
    is_flag=True,
    help="Instruct visualitation to connect to simulation server instead of orchestrator²",
)
def run_visualisation(ctx, config, sim):
    """Start a StorAlloc visualisation server (based on Bokeh), which traces events
    from either orchestrator (live) or simulation server (when simulation run is triggered)"""

    click.secho("[~] Starting visualisation server.", fg="green")
    vis = visualisation.Visualisation(config, verbose=ctx.obj["verbose"])
    vis.run()


if __name__ == "__main__":

    cli(obj={})  # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
