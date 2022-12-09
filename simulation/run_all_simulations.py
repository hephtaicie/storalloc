#!/usr/bin/env python

"""
Run a set of simulation using every possible combinations of given
configuration files, data files, and storage system description files.

Beware : May take a long time to run depending on the size of the dataset.

"""

import itertools
import datetime as dt
import time
from pathlib import Path

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


def prepare_logs_directory():
    """Create a  timestamped directory in "../results" where
    results from this experiment run will be stored
    """

    dir_name = f"logs__{dt.datetime.now().strftime('%d-%b-%y_%H-%M')}"

    logs_path = Path(f"./logs/{dir_name}")
    if logs_path.exists():
        raise RuntimeError(f"Logs directory {dir_name} already exists !")

    logs_path.mkdir()
    return str(logs_path)


if __name__ == "__main__":

    BASE_PATH_CONFIG = "../config"

    BASE_PATH_SYSTEM = [
        f"{BASE_PATH_CONFIG}/systems/infra8TB",
        f"{BASE_PATH_CONFIG}/systems/infra16TB",
        f"{BASE_PATH_CONFIG}/systems/infra32TB",
        #f"{BASE_PATH_CONFIG}/systems/infra32TBBB",
        # f"{BASE_PATH_CONFIG}/systems/infra64TBBB",
        f"{BASE_PATH_CONFIG}/systems/infra64TB",
    ]

    CONFIG_DETAILS = "split_200G"  # split_100T ; split_100T_retry ; split_200G ; split_200G_retry
    CONFIG_FILES = [
        f"{BASE_PATH_CONFIG}/{CONFIG_DETAILS}/{algo}"
        for algo in [
            "config_worst_case.yml",
            # "config_random.yml",
            # "config_rr.yml",
            # "config_worst_fit.yml",
        ]
    ]

    SYSTEM_FILES = [f"{base_path}/multi_node_multi_disk.yml" for base_path in BASE_PATH_SYSTEM]
    SYSTEM_FILES += [f"{base_path}/single_node_multi_disk.yml" for base_path in BASE_PATH_SYSTEM]
    SYSTEM_FILES += [f"{base_path}/multi_node_single_disk.yml" for base_path in BASE_PATH_SYSTEM]
    SYSTEM_FILES += [f"{base_path}/single_node_single_disk.yml" for base_path in BASE_PATH_SYSTEM]

    BASE_PATH_DATA = "../data"
    JOB_FILES = [
        f"{BASE_PATH_DATA}/IOJobs.yml",
    ]

    EXP_DIR = prepare_result_directory()
    LOGS_DIR = prepare_logs_directory()
    print(f"## Directory for experiment results is : {EXP_DIR}")
    print(f"## Using SYSTEM FILES : {SYSTEM_FILES}")
    print(f"## Using JOB_FILES : {JOB_FILES}")
    print(f"## Using CONFIG_FILES : {CONFIG_FILES}")
    print(f"## Using CONFIG_DETAILS : {CONFIG_DETAILS}")
    print()
    print("___________________________________________________________________")

    for permutation in itertools.product(CONFIG_FILES, SYSTEM_FILES, JOB_FILES):

        config_file, system_file, job_file = permutation
        start = time.time()
        run_exp(EXP_DIR, config_file, system_file, job_file, LOGS_DIR)
        duration = time.time() - start
        print(f"DURATION FOR THIS EXP: {duration}s")
        print("___________________________________________________________________")
