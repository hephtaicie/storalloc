#!/usr/bin/env python

"""
Run a set of simulation using every possible combinations of given
configuration files, data files, and storage system description files.

Beware : May take a long time to run depending on the size of the dataset.

"""

import subprocess
import shutil
from pathlib import Path
import signal
import time


def remove_output():
    """Cleanup local directory by removing any existing 'output.yml'
    We do this prior to running the experiment in order to be safe
    if the experiment fails (we don't want to mistage an old result
    for the failed experiment result)
    """

    output_path = Path("./output.yml")
    if output_path.exists():
        output_path.unlink()
        print("Removed a local copy of output.yml before running experiment")
    else:
        print("No local copy of output.yml found")


def copy_results(exp_dir, algo, infra, jobs):
    """Copy experiment result to the correct directory"""

    output_path = Path("./output.yml")
    if not output_path.exists():
        raise RuntimeError(
            f"Couldn't find a result file for experiment with algo {algo} and infra {infra}"
        )

    new_path = Path(exp_dir).joinpath(Path(f"exp__{algo}_{infra}_{jobs}.yml"))
    shutil.move(str(output_path), str(new_path))


def run_exp(exp_dir, config_file, system_file, job_file):
    """Run simulation"""

    print(f"## Running simulation with config {config_file} / {system_file} / {job_file}")

    remove_output()

    # Start a sim-server :
    sim_server = subprocess.Popen(
        ["storalloc", "sim-server", "-c", config_file],
        stdout=subprocess.DEVNULL,
        # stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    print(f"Started subprocess sim_server with PID {sim_server.pid}")

    # Start an orchestrator :
    orchestrator = subprocess.Popen(
        ["storalloc", "orchestrator", "-c", config_file],
        stdout=subprocess.DEVNULL,
        # stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    print(f"Started subprocess orchestrator with PID {orchestrator.pid}")

    # Start a server :
    server = subprocess.Popen(
        ["storalloc", "server", "-c", config_file, "-s", system_file],
        stdout=subprocess.DEVNULL,
        # stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    print(f"Started subprocess server with PID {server.pid}")

    # Start a sim-client :
    sim_client = subprocess.Popen(
        ["storalloc", "sim-client", "-c", config_file, "-j", job_file],
        stdout=subprocess.DEVNULL,
        # stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    print(f"Started subprocess sim_client with PID {sim_client.pid}")

    while True:
        if sim_server.poll() is not None:
            print(f"Sim server {sim_server.pid} terminated")
            break
        time.sleep(5)

    orchestrator.send_signal(signal.SIGINT)  # for subprocess clean up
    server.kill()
    sim_client.kill()
    time.sleep(2)

    copy_results(
        exp_dir,
        Path(config_file).stem.lstrip("config_"),
        f"{Path(system_file).parent.stem}_{Path(system_file).stem}",
        Path(job_file).stem,
    )


if __name__ == "__main__":

    import sys

    run_exp(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
