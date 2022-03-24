#!/usr/bin/env python

"""
Run a set of simulation using every possible combinations of given
configuration files, data files, and storage system description files.

Beware : May take a long time to run depending on the size of the dataset.

"""

import itertools
import datetime as dt
import subprocess
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


def prepare_result_directory():
    """Create a  timestamped directory in "../results" where
    results from this experiment run will be stored
    """

    dir_name = f"exp__{dt.datetime.now().strftime('%d-%b-%y_%H-%M')}"

    results_path = Path(f"./results/{dir_name}")
    if results_path.exists():
        raise RuntimeError(f"Result directory {dir_name} already exists !")

    results_path.mkdir()
    return str(results_path)


def copy_results(exp_dir, algo, infra, jobs):
    """Copy experiment result to the correct directory"""

    output_path = Path("./output.yml")
    if not output_path.exists():
        raise RuntimeError(
            f"Couldn't find a result file for experiment with algo {algo} and infra {infra}"
        )

    new_path = Path(exp_dir).joinpath(Path(f"exp__{algo}_{infra}_{jobs}.yml"))
    output_path.replace(new_path)


def run_exp(exp_dir, config_file, system_file, job_file):
    """Run simulation"""

    print(f"## Running simulation with config {config_file} / {system_file} / {job_file}")

    remove_output()

    # Start a sim-server :
    sim_server = subprocess.Popen(
        ["storalloc", "sim-server", "-c", config_file],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    print(f"Started subprocess sim_server with PID {sim_server.pid}")

    # Start an orchestrator :
    orchestrator = subprocess.Popen(
        ["storalloc", "orchestrator", "-c", config_file],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    print(f"Started subprocess orchestrator with PID {orchestrator.pid}")

    # Start a server :
    server = subprocess.Popen(
        ["storalloc", "server", "-c", config_file, "-s", system_file],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    print(f"Started subprocess server with PID {server.pid}")

    # Start a sim-client :
    sim_client = subprocess.Popen(
        ["storalloc", "sim-client", "-c", config_file, "-j", job_file],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
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

    BASE_PATH_CONFIG = "../config"

    BASE_PATH_SYSTEM = [
        f"{BASE_PATH_CONFIG}/systems/infra8TB",
        f"{BASE_PATH_CONFIG}/systems/infra16TB",
        f"{BASE_PATH_CONFIG}/systems/infra64TB",
    ]

    CONFIG_DETAILS = "split_100T"
    CONFIG_FILES = [
        f"{BASE_PATH_CONFIG}/{CONFIG_DETAILS}/{algo}"
        for algo in [
            "config_worst_case.yml",
            "config_random.yml",
            "config_rr.yml",
            "config_worst_fit.yml",
        ]
    ]

    # I was too lazy to add a damn loop, and copy paste is so fast in vim...
    SYSTEM_FILES = [f"{base_path}/mutli_node_multi_disk.yml" for base_path in BASE_PATH_SYSTEM]
    SYSTEM_FILES += [f"{base_path}/single_node_multi_disk.yml" for base_path in BASE_PATH_SYSTEM]
    SYSTEM_FILES += [f"{base_path}/multi_node_single_disk.yml" for base_path in BASE_PATH_SYSTEM]
    SYSTEM_FILES += [f"{base_path}/single_node_single_disk.yml" for base_path in BASE_PATH_SYSTEM]

    BASE_PATH_DATA = "../data"
    JOB_FILES = [
        f"{BASE_PATH_DATA}/IOJobsOct.yml",
    ]

    EXP_DIR = prepare_result_directory()
    print(f"## Directory for experiment results is : {EXP_DIR}")
    print(f"## Using SYSTEM FILES : {SYSTEM_FILES}")
    print(f"## Using JOB_FILES : {JOB_FILES}")
    print(f"## Using CONFIG_FILES : {CONFIG_FILES}")
    print("___________________________________________________________________")

    for permutation in itertools.product(CONFIG_FILES, SYSTEM_FILES, JOB_FILES):

        config_file, system_file, job_file = permutation
        run_exp(EXP_DIR, config_file, system_file, job_file)
