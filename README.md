[![pipeline status](https://gitlab.inria.fr/Kerdata/kerdata-projects/storalloc/badges/develop/pipeline.svg)](https://gitlab.inria.fr/Kerdata/kerdata-projects/storalloc/-/commits/develop)
[![coverage report](https://gitlab.inria.fr/Kerdata/kerdata-projects/storalloc/badges/develop/coverage.svg)](https://gitlab.inria.fr/Kerdata/kerdata-projects/storalloc/-/commits/develop)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Storalloc - A Scheduler for Storage Resources

Storalloc is a prototype of a job scheduler for storage resources. While compute resources are usually allocatable exclusively on a HPC system, storage resources are still either a global and shared file system or distributed intermediate resources difficult to effectively use. We propose here a proof-of-concept of a scheduler based on a three-component design (client, orchestrator, server) for allocating storage space the same way we allocate compute resources on a supercomputer. Storalloc can also run locally in a simulation mode for testing scheduling algorithms or playing traces ([Darshan](https://www.mcs.anl.gov/research/projects/darshan/) support in progress). Our prototype can now support NVMeoF technology to attach storage resources through a high-speed network. 

## Design

![Storalloc design](doc/img/StorAlloc_design.png)

## Installation & requirements

Full list of current requirements can be found in the `setup.py`. 

This package is not yet released on Pypi, but you can install it from the repository using: 

```shell
$ pip install --upgrade pip # optionnal
$ pip install -e .          # -e is for development mode (a.k.a 'editable')
```

or

```shell
$ pip install --upgrade pip # optionnal
$ pip install -e '.[dev]' # if development dependencies are required as well.
```

Storalloc uses the [ZeroMQ](https://zeromq.org/) messaging library to implement the communication layer between components. ZeroMQ is an active open source project (LGPL) that offers an advanced communication API built on top of sockets. The Python bindings and the library can be easily installed on most systems through the package manager (`python3-zmq` on Debian for instance) or via `pip`.
Storalloc also uses the [Simpy](https://simpy.readthedocs.io/en/latest/) DES library. Simpy is an active open source project (MIT Licence) which leverages Python generator functions into a discret-event simulation framework.

Note that a few other Python dependencies are such as `yaml` and `pandas` are needed for some scripts presented in this repository (eg. data extraction from traces).

A snapshot of the current environment being used in the development of storalloc can be found in `requirements.txt`

## Simulation mode

Storalloc interactive simulation mode requires the use of the following components:

- `client`
- `server` (one or more, with `--simulate` flag)
- `orchestrator`
- `sim-server` (optional, but recommended)
- `visualisation` (optional, but recommended)
- `log-server` (optional, but recommended)

Using this operating mode, Storalloc won't allocate storage, and client(s) will receive a dummy response for each request. A single node is enough to run all components collocated.
Below is a simple scenario executed on a local machine, configured to use a random allocation scheduler (baseline implementation). 

###### Logging

First we start a `log-server`, which aggregates and displays logs from other components.
This is optional and every component can also log locally, to console and/or file, depending on its configuration.

```shell
$ storalloc log-server -c config/config_random.yml
```

###### Simulation

Setup our simulation server, in charge of collecting events and later on, to run the simulation.
We also start a visulation server running a [Bokeh](https://bokeh.org/) app.
During the simulation run, the Bokeh app receives messages with datapoints that need to be graphed, and renders on various plots.
This is not a mandatory component if simulation metrics are not needed.

```shell
$ storalloc sim-server -c config/config_random.yml -l
```

`-l` must be added for remote logging, which is not the default for this component.

```shell
$ storalloc visualisation -c config/config_random.yml
```

When a Ctrl-C is issued on the simulation server, it will stop collecting events and start running the simulation.
At this point, if any visualisation server is running, it will receives updates from the simulation and start plotting.

Design note: for this specific use case, we'd want the visualisation server to be started by the simulation when it starts.
This is currently under work.

###### Core components 

The bare minimum components required for Storalloc execution are the orchestrator, the server(s) and the client(s).
We start them in this order.

```shell
$ storalloc orchestrator -c config/config_random.yml
```

```shell
$ storalloc server -c config/config_random.yml -s config/systems/ault14.yml
```
(use as many as needed, started in separate shells)

```shell
$ storalloc client -c config/config_random.yml -s 200 -t 1:00:00
```
(request for 200GB, for one hour, starting now)

## Replay traces

As seen in the example from previous section, Storalloc can be run interactively in order to feed a simulation with manually crafted requests. A second simulation mode allows to replay actual I/O traces from Darshan logs.

While developing Storalloc, we have so far used data from the Theta system : [Darshan dataset](https://reports.alcf.anl.gov/data/theta.html) of Theta*, a 10PFlops system at Argonne National Laboratory from Jan. 1st 2020 to Dec. 31st of the same year.

Traces are processed by the script `./simulation/traces/extract_traces.py` and we use the resulting yaml file (`IOJobs.yml` when using data from a whole year) as input for the simulation. Doing so requires the use of a specific simulation client.

Below is an example of replaying I/O traces :

###### Logging

Unchanged from previous section

###### Simulation

Unchanged from previous section

###### Core components

The bare minimum components required for Storalloc execution are the orchestrator, the server(s) and the client(s).
We start them in this order.

```shell
$ storalloc orchestrator -c config/config_random.yml
```

```shell
$ storalloc server -c config/config_random.yml -s config/systems/ault14.yml
```
(use as many as needed, started in separate shells)

```shell
$ storalloc sim-client -c config/config_random.yml -j data/IOJobs.yml
```
(data path is an example, but you should use your own locally-generated `IOJobs.yml` file)




* This data was generated from resources of the Argonne Leadership Computing Facility, which is a DOE Office of Science User Facility supported under Contract DE-AC02-06CH11357. 
