# Storalloc - A Scheduler for Storage Resources

Storalloc is a prototype of a job scheduler for storage
resources. While compute resources are usually allocatable exclusively
on a HPC system, storage resources are still either a global and
shared file system or distributed intermediate resources difficult to
effectively use. We propose here a proof-of-concept of a scheduler
based on a three-component design (client, orchestrator, server) for
allocating storage space the same way we allocate compute resources on
a supercomputer. Storalloc can also run locally in a simulation mode
for testing scheduling algorithms or playing traces (Darshan support
in progress). Our prototype can now support NVMeoF technology to attach
storage resources through a high-speed network.

## Design

![Storalloc design](doc/img/StorAlloc_design.png)
