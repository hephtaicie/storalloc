#!/usr/bin/env python3

"""
Running the simulations on Grid5000 clusters
"""

import sys
from pathlib import Path
import itertools
import datetime as dt
import numpy as np
import enoslib as en

WORK_DIR = "/tmp/storalloc"

BASE_PATH_CONFIG = f"{WORK_DIR}/config"

BASE_PATH_SYSTEM = [
    f"{BASE_PATH_CONFIG}/systems/infra8TB",
    #    f"{BASE_PATH_CONFIG}/systems/infra16TB",
    #    f"{BASE_PATH_CONFIG}/systems/infra64TB",
]

# CONFIG_OPTIONS = ["split_100T", "split_100G", "split_100T_retry", "split_100G_retry"]
CONFIG_OPTIONS = ["split_100T"]
CONFIG_FILES = [
    f"{BASE_PATH_CONFIG}/{opt}/{algo}"
    for algo, opt in itertools.product(
        [
            "config_worst_case.yml",
            "config_random.yml",
            "config_rr.yml",
            "config_worst_fit.yml",
        ],
        CONFIG_OPTIONS,
    )
]

# I was too lazy to add a damn loop, and copy paste is so fast in vim...
SYSTEM_FILES = [f"{base_path}/mutli_node_multi_disk.yml" for base_path in BASE_PATH_SYSTEM]
# SYSTEM_FILES += [f"{base_path}/single_node_multi_disk.yml" for base_path in BASE_PATH_SYSTEM]
# SYSTEM_FILES += [f"{base_path}/multi_node_single_disk.yml" for base_path in BASE_PATH_SYSTEM]
# SYSTEM_FILES += [f"{base_path}/single_node_single_disk.yml" for base_path in BASE_PATH_SYSTEM]

BASE_PATH_DATA = "/home/jmonniot/StorAlloc/data"
JOB_FILES = [
    f"{BASE_PATH_DATA}/IOJobsOct.yml",
]


PERMUTATIONS = list(itertools.product(CONFIG_FILES, SYSTEM_FILES, JOB_FILES))

TASKS_PER_NODE = 2
CLUSTER = "parasilo"


def results_dir_name():
    """Generate a timestampd directory name including compute host information"""
    dir_name = f"exp__{dt.datetime.now().strftime('%d-%b-%y_%H-%M')}"
    results_path = Path(f"/home/jmonniot/StorAlloc/results/{dir_name}")
    return str(results_path)


def run_node(job_name: str, walltime: str, params: list):
    """Run a few simulations (whose parameters are given in a list) sequentially on a node"""

    ## Prepare Configuration
    prod_network = en.G5kNetworkConf(
        id="net_storalloc",
        type="prod",
        roles=["storalloc_net"],
        site="rennes",
    )

    conf = (
        en.G5kConf.from_settings(
            job_name=job_name,
            walltime=walltime,
            job_type=["allow_classic_ssh"],
            key=str(Path.home() / ".ssh" / "id_rsa_grid5000.pub"),
        )
        .add_network_conf(prod_network)
        .add_machine(
            roles=["compute_storalloc"],
            cluster=CLUSTER,
            nodes=1,
            primary_network=prod_network,
        )
        .finalize()
    )

    # Use the correct SSH key (in my case it's not the regular id_rsa.pub)
    conf.key = str(Path.home() / ".ssh" / "id_rsa_grid5000.pub")

    provider = en.G5k(conf)
    roles, network = provider.init()
    roles = en.sync_info(roles, network)

    for node in roles["compute_storalloc"]:
        print(type(node))
        print(node)

    en.ensure_python3(roles=roles)

    ### ACTIONS
    """
    with en.actions(
        roles=roles,
        gather_facts=True,
        extra_vars={"remote_user": "jmonniot", "ansible_ssh_private_key_file": conf.key[:-4]},
    ) as play:

        play.apt(name="git", state="present")
        play.git(
            repo="https://oauth2:glpat-W7MXcHS1tkjmr7JAr95H@gitlab.inria.fr/Kerdata/kerdata-projects/storalloc.git",
            dest="/tmp/storalloc",
            depth=1,
            version="develop",
        )
        play.pip(
            chdir="/tmp/storalloc",
            name=".",
            virtualenv="storalloc_venv",
        )
        for param in params:
            play.command(
                chdir="/tmp/storalloc/simulation",
                cmd=f"../storalloc_venv/bin/python3 run_simulation.py {' '.join(param)}",
            )
    """

    provider.destroy()


def split_permutations(permutations: list, split_size: int):
    """Split the list of permutations into multiple sublists"""

    for idx in range(0, len(permutations), split_size):
        yield permutations[idx : idx + split_size]


def run():
    """Main"""

    print(f"There will be {len(PERMUTATIONS)} simulations to run on {CLUSTER}")
    nb_nodes = int(len(PERMUTATIONS) / TASKS_PER_NODE)
    print(f"They will run on {nb_nodes} nodes")

    results_dir = results_dir_name()

    run_node("storalloc_test", "00:05:00", PERMUTATIONS)

    """
    job_idx = 0
    for param_set in split_permutations(PERMUTATIONS, TASKS_PER_NODE):
        print(param_set)
        for param in param_set:
            param = list(param)
            param.insert(0, results_dir)
            job_name = f"storalloc_{job_idx}"
            run_node(job_name, "00:05:00", param)
            job_idx += 1
        print("_________________")
    """


if __name__ == "__main__":
    _ = en.init_logging()
    run()
