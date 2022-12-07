#!/usr/bin/env python3

""" Running the simulations on Grid5000 clusters

    This script is specifically used on the Paravance cluster at INRIA Rennes
    and would probably need some updates for a more general use.

    You will most certainly need to update a few paths and configurations in the
    global variables below.

"""

from pathlib import Path
import itertools
import datetime as dt
import enoslib as en

WORK_DIR = "/tmp/storalloc"

REPO_URL = "gitlab.inria.fr/Kerdata/kerdata-projects/storalloc.git"
REPO_KEY = ""

# Where to find StorAlloc configuration files
BASE_PATH_CONFIG = f"{WORK_DIR}/config"

# Which storage system SIZE to use
BASE_PATH_SYSTEM = [
    f"{BASE_PATH_CONFIG}/systems/infra8TB",
    f"{BASE_PATH_CONFIG}/systems/infra16TB",
    f"{BASE_PATH_CONFIG}/systems/infra32TB",
    f"{BASE_PATH_CONFIG}/systems/infra64TB",
]

# Which strategies to use
# CONFIG_OPTIONS = ["split_100T", "split_200G", "split_100T_retry", "split_200G_retry"]
# CONFIG_OPTIONS = ["split_100T", "split_100T_retry"]
CONFIG_OPTIONS = ["split_200G", "split_200G_retry"]
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

# Which storage system LAYOUT to use
# I was too lazy to add a damn loop, and copy paste is so fast in vim...
SYSTEM_FILES = [f"{base_path}/multi_node_multi_disk.yml" for base_path in BASE_PATH_SYSTEM]
SYSTEM_FILES += [f"{base_path}/single_node_multi_disk.yml" for base_path in BASE_PATH_SYSTEM]
SYSTEM_FILES += [f"{base_path}/multi_node_single_disk.yml" for base_path in BASE_PATH_SYSTEM]
SYSTEM_FILES += [f"{base_path}/single_node_single_disk.yml" for base_path in BASE_PATH_SYSTEM]

# Where is the data file
BASE_PATH_DATA = "/home/jmonniot/StorAlloc/data"
# Which data file to use
JOB_FILES = [
    f"{BASE_PATH_DATA}/IOJobs.yml",
]


PERMUTATIONS = list(itertools.product(CONFIG_FILES, SYSTEM_FILES, JOB_FILES))
MAX_TASKS_PER_NODE = 4
MAX_NODES = 32
CLUSTER = "paravance"


def results_dir_name():
    """Generate a timestampd directory name including compute host information"""
    dir_name = f"exp__{dt.datetime.now().strftime('%d-%b-%y_%H-%M')}"
    results_path = Path(f"/home/jmonniot/StorAlloc/results/{dir_name}")
    return str(results_path)


def split_permutations(permutations: list, split_size: int):
    """Split the list of permutations into multiple sublists"""

    for idx in range(0, len(permutations), split_size):
        yield permutations[idx : idx + split_size]


def prepare_params():
    """Prepare a list of parameters for each simulation to be run"""

    results_dir = results_dir_name()
    print(f"There will be {len(PERMUTATIONS)} simulations to run on {CLUSTER}")
    nb_nodes = int(len(PERMUTATIONS) / MAX_TASKS_PER_NODE)
    if nb_nodes > MAX_NODES:
        nb_nodes = MAX_NODES
    nb_tasks_per_node = int(len(PERMUTATIONS) / nb_nodes)
    print(f"They will run on {nb_nodes} nodes ({nb_tasks_per_node} tasks per node)")

    params = []
    for idx, param_set in enumerate(split_permutations(PERMUTATIONS, nb_nodes)):
        params.append([])
        for param in param_set:
            param = list(param)
            param.insert(0, results_dir)
            params[idx].append(param)

    return params


def run_batch(node_number: int, params: list, results_dir: str):
    """Run a few simulations (whose parameters are given in a list) on a set of nodes"""

    ## Prepare Configuration
    prod_network = en.G5kNetworkConf(
        id="net_storalloc",
        type="prod",
        roles=["storalloc_net"],
        site="rennes",
    )

    conf = (
        en.G5kConf.from_settings(
            job_name="storalloc_sim",
            walltime="02:00:00",
            job_type=["allow_classic_ssh"],
            # key=str(Path.home() / ".ssh" / "id_rsa_grid5000.pub"),
        )
        .add_network_conf(prod_network)
        .add_machine(
            roles=["compute_storalloc"],
            cluster=CLUSTER,
            nodes=node_number,
            primary_network=prod_network,
        )
        .finalize()
    )

    provider = en.G5k(conf)
    roles, network = provider.init()
    roles = en.sync_info(roles, network)

    ### Prepare parameters
    for node, param in zip(roles["compute_storalloc"], params):
        node.extra = {"storalloc_params": param}

    en.ensure_python3(roles=roles)

    ### ACTIONS
    results = None
    with en.actions(
        roles=roles,
        gather_facts=True,
    ) as play:

        play.apt(name="git", state="present")
        play.git(
            repo=f"https://oauth2:{REPO_KEY}@{REPO_URL}",
            dest="/tmp/storalloc",
            depth=1,
            version="feature-g5ksim",
        )
        play.pip(
            chdir="/tmp/storalloc",
            name=".",
            virtualenv="storalloc_venv",
        )
        play.shell(
            command="echo '{{ storalloc_params  }}' "
            + ">> /home/jmonniot/StorAlloc/results/{{ ansible_hostname }}_params.txt"
        )
        play.file(path=results_dir, state="directory")
        play.shell(
            chdir="/tmp/storalloc",
            command=". storalloc_venv/bin/activate &&"
            + " cd simulation &&"
            + " ./run_simulation.py {{ storalloc_params }}",
        )
        results = play.results

    for result in results:
        print(result)
        print("################################################################################")

    return provider


def run():
    """Main"""

    if REPO_KEY == "":
        print(
            "Update the script by adding a deployment key for the"
            "targetted repository before running"
        )
        return

    params = prepare_params()
    results_dir = params[0][0][0]
    provider = None

    for idx, batch in enumerate(params):
        print(f"Batch {idx} :: ")
        batch_params = []
        for node_idx, param_set in enumerate(batch):
            print(f" Node {node_idx} :: Params : {param_set}")
            str_params = " ".join(param_set)
            batch_params.append(str_params)

        print(f"Starting batch with {len(batch)} nodes")
        provider = run_batch(len(batch), batch_params, results_dir)

    provider.destroy()


if __name__ == "__main__":
    _ = en.init_logging()
    run()
