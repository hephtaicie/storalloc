## Server
* Test `delete_disk_allocation`
* [DONE] Send subsystem the NVMe Qualified Name (NQN ~ subsystem id) to the client

## Client
* [DONE] Receive NQN and connect NVMe remote storage target
* Disconnect all subsystems
  * Save and load status of connected devices
* Bug fix "resourcesGranted job allocation"
* Set up a default path for the client config file

## Orchestrator

## All
* Better requests handling (comma-seperated list so far)
