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


def copy_results(exp_dir, algo, split, infra, jobs):
    """Copy experiment result to the correct directory"""

    output_path = Path("./output.yml")
    if not output_path.exists():
        raise RuntimeError(
            f"Couldn't find a result file for experiment with algo {algo} and infra {infra}"
        )

    new_path = Path(exp_dir).joinpath(Path(f"exp__{split}_{algo}_{infra}_{jobs}.yml"))
    shutil.move(str(output_path), str(new_path))


def run_exp(exp_dir, config_file, system_file, job_file, logs_dir):
    """Run simulation"""

    print(f"## Running simulation with config {config_file} / {system_file} / {job_file}")

    remove_output()

    algo = Path(config_file).stem.lstrip("config_")
    split = Path(config_file).parent.stem
    infra = f"{Path(system_file).parent.stem}_{Path(system_file).stem}"
    jobs = Path(job_file).stem

    # Start a sim-server :
    sim_server_log_path = Path(f"{logs_dir}/sim_server.log")
    sim_server_log_file = open(sim_server_log_path, "w", encoding="utf-8")
    sim_server = subprocess.Popen(
        ["storalloc", "sim-server", "-c", config_file],
        stdout=sim_server_log_file,
        stderr=sim_server_log_file,
    )
    time.sleep(2)
    print(f"Started subprocess sim_server with PID {sim_server.pid}")

    # Start an orchestrator :
    orchestrator_log_path = Path(f"{logs_dir}/orchestrator.log")
    orchestrator_log_file = open(orchestrator_log_path, "w", encoding="utf-8")
    orchestrator = subprocess.Popen(
        ["storalloc", "orchestrator", "-c", config_file],
        stdout=orchestrator_log_file,
        stderr=orchestrator_log_file,
    )
    time.sleep(2)
    print(f"Started subprocess orchestrator with PID {orchestrator.pid}")

    # Start a server :
    server_log_path = Path(f"{logs_dir}/server.log")
    server_log_file = open(server_log_path, "w", encoding="utf-8")
    server = subprocess.Popen(
        ["storalloc", "server", "-c", config_file, "-s", system_file],
        stdout=server_log_file,
        stderr=server_log_file,
    )
    time.sleep(2)
    print(f"Started subprocess server with PID {server.pid}")

    # Start a sim-client :
    client_log_path = Path(f"{logs_dir}/client.log")
    client_log_file = open(client_log_path, "w", encoding="utf-8")
    sim_client = subprocess.Popen(
        ["storalloc", "sim-client", "-c", config_file, "-j", job_file],
        stdout=client_log_file,
        stderr=client_log_file,
    )
    time.sleep(2)
    print(f"Started subprocess sim_client with PID {sim_client.pid}")

    while True:
        if sim_server.poll() is not None:
            print(f"## SIM-SERVER {sim_server.pid} TERMINATED")
            break
        time.sleep(10)

    orchestrator.send_signal(signal.SIGINT)  # for subprocess clean up
    server.kill()
    sim_client.kill()
    time.sleep(3)

    copy_results(exp_dir, algo, split, infra, jobs)


if __name__ == "__main__":

    import sys

    start = time.time()
    run_exp(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    duration = time.time() - start
    print(f"Duration for options {sys.argv[1:]} = {duration}")
