""" Storalloc
    ConfigFile parser
"""

import yaml

import cerberus

from storalloc.logging import get_storalloc_logger

# Cerberus

CONFIG_SCHEMA = {
    "orchestrator_hostname": {"type": "str"},
    "orchestrator_addr": {"type": "str"},
    "orchestrator_fe_ipc": {"type": "str"},
    "orchestrator_be_ipc": {"type": "str"},
    "client_port": {"type": "int", "min": 1025, "max": 65535, "required": False},
    "server_port": {"type": "int", "min": 1025, "max": 65535, "required": False},
    "transport": {"type": "str", "allowed": ["tcp", "ipc"]},
    "sched_strategy": {"type": "str", "allowed": ["random_alloc", "worst_case"]},
    "simulation": {
        {
            "type": "dict",
            "schema": {
                "nb_clients": {"type": "int"},
                "nb_servers": {"type": "int"},
            },
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
