""" Storalloc
    CLI entrypoint
"""
import datetime
import click

from storalloc import client, server, orchestrator


@click.group()
@click.pass_context
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose mode")
def cli(ctx, verbose):
    """Storalloc CLI"""
    click.secho("## Storalloc ##", fg="cyan")
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


# SERVER
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
    click.secho("[~] Starting server...", fg="green")
    server.run(config, system, reset, simulate)


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
@click.option(
    "--simulate",
    is_flag=True,
    help="Simulation mode for replaying traces",
)
def run_orchestrator(ctx, config, simulate):
    """Orchestrator command"""
    click.secho("[~] Starting orchestrator...", fg="green")
    orches = orchestrator.Orchestrator(config, simulate)
    orches.run()


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

    click.secho("[~] Starting client...", fg="green")
    # Convert duration of requested storage allocation to seconds
    time_delta = int(
        datetime.timedelta(
            hours=time.hour, minutes=time.minute, seconds=time.second
        ).total_seconds()
    )
    client.run(config, size, time_delta, start_time, eos)


if __name__ == "__main__":
    """CLI Entrypoint"""
    cli(obj={})
