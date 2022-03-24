#!/usr/bin/env python3

"""
Running the simulations on Grid5000 clusters
"""

from pathlib import Path

import enoslib as en


def run():
    """Main"""

    prod_network = en.G5kNetworkConf(
        id="net_storalloc", type="prod", roles=["my_network"], site="rennes"
    )

    conf = (
        en.G5kConf.from_settings(
            job_name="storalloc_sim", walltime="00:05:00", job_type=["allow_classic_ssh"]
        )
        .add_network_conf(prod_network)
        .add_machine(roles=["compute"], cluster="parasilo", nodes=3, primary_network=prod_network)
        .finalize()
    )

    # Use the correct SSH key (in my case it's not the regular id_rsa.pub)
    conf.key = str(Path.home() / ".ssh" / "id_rsa_grid5000.pub")

    provider = en.G5k(conf)
    roles, network = provider.init()

    print(provider.hosts)
    print(network)

    res = en.api.run_command(
        command="ls /tmp",
        extra_vars={"remote_user": "jmonniot", "ansible_ssh_private_key_file": conf.key[:-4]},
        pattern_hosts="compute",
        roles=roles,
        task_name="Dummy ls"
    )
    print(res)

    provider.destroy()


if __name__ == "__main__":

    _ = en.init_logging()

    run()
