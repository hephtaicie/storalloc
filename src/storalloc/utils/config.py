""" Storalloc
    ConfigFile parser
"""

import yaml

import cerberus

from storalloc.utils.logging import get_storalloc_logger  # Cerberus

CONFIG_SCHEMA = {
    "orchestrator_hostname": {"type": "string"},
    "orchestrator_addr": {"type": "string"},
    "orchestrator_fe_ipc": {"type": "string"},
    "orchestrator_be_ipc": {"type": "string"},
    "client_port": {"type": "integer", "min": 1025, "max": 65535},
    "server_port": {"type": "integer", "min": 1025, "max": 65535},
    "log_server_addr": {"type": "string"},
    "log_server_port": {"type": "integer", "min": 1025, "max": 65535},
    "log_server_sync_port": {"type": "integer", "min": 1025, "max": 65535},
    "simulation_addr": {"type": "string"},
    "simulation_port": {"type": "integer", "min": 1025, "max": 65535},
    "o_visualisation_port": {"type": "integer", "min": 1025, "max": 65535},
    "s_visualisation_port": {"type": "integer", "min": 1025, "max": 65535},
    "transport": {"type": "string", "allowed": ["tcp", "ipc"]},
    "sched_strategy": {"type": "string", "allowed": ["random_alloc", "worst_case"]},
    "res_catalog": {
        "type": "string",
        "allowed": [
            "inmemory",
        ],
    },
    "simulation": {
        "type": "dict",
        "schema": {
            "nb_clients": {"type": "integer"},
            "nb_servers": {"type": "integer"},
        },
    },
}


def config_from_yaml(path: str):
    """Load a storalloc configuration from a given YAML file"""

    log = get_storalloc_logger()
    config = {}

    try:
        with open(path, "r", encoding="utf-8") as yaml_stream:
            config = yaml.safe_load(yaml_stream)
    except (FileNotFoundError, PermissionError) as exc:
        log.error(f"Unable to open configuration file : {exc}")
        raise

    validator = cerberus.Validator(CONFIG_SCHEMA)

    if not validator.validate(config):
        log.error(f"Schema validation failed for config : {validator.errors}")
        raise ValueError(f"Configuration loading failed (schema has error(s) : {validator.errors}")

    return config
