""" Storalloc
    CLI entrypoint
"""

import click

from storalloc import client, server, orchestrator


@click.group()
def cli():
    """Main command"""
    click.secho("## Storalloc ##", fg="cyan")


@cli.command()
def server():
    """Server command"""
    click.secho("[~] Starting server...", fg="green")


@cli.command()
def orchestrator():
    """Orchestrator command"""
    click.secho("[~] Starting orchestrator...", fg="green")


@cli.command()
def client():
    """Client command"""
    click.secho("[~] Starting client...", fg="green")


if __name__ == "__main__":
    """CLI Entrypoint"""
    cli()
