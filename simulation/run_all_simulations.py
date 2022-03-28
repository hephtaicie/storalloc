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

from run_simulation import run_exp


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
