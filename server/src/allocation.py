#!/usr/bin/env python3

import logging
import parted
import sys
import time
from src.nvmet import nvme

current_port_id = 1
current_port    = 4420

class Allocation (object):

    def __init__ (self):
        super().__init__()
        self._cfnode      = nvme.Root()
        self._subsystem   = None
        self._namespace   = None
        self._port        = None
        self._subsys_port = 0
        
        self._part_dev  = None
        self._dev_nvme  = None
        self._disk_nvme = None
        self._partition = None

    def create_disk_allocation (self, node_ipv4, device, capacity):
        global current_port, current_port_id
        
        # Partition disk
        # See: https://gist.github.com/herry13/5931cac426da99820de843477e41e89e
        self._dev_nvme   = parted.getDevice (device)
        self._disk_nvme  = parted.newDisk (self._dev_nvme)

        free_space = self._disk_nvme.getFreeSpaceRegions()
        sectors_required = parted.sizeToSectors(capacity, "GiB", self._dev_nvme.sectorSize)

        # TODO: cost model for partition placement
        space = None
        for idx, g in enumerate(free_space):
            if g.length > sectors_required:
                space = idx
                break

        if space is None:
            # TODO: Raise exception instead that can be catched in the calls stack
            print ("Error: No contiguous free space available on "+device)
            sys.exit(1)
        else:
            geometry   = parted.Geometry(start=free_space[space].start,
                                       length=sectors_required, device=self._dev_nvme)
            filesystem = parted.FileSystem(type='xfs', geometry=geometry)

            self._partition  = parted.Partition(disk=self._disk_nvme,
                                                type=parted.PARTITION_NORMAL,
                                                fs=filesystem,
                                                geometry=geometry)
            
            self._disk_nvme.addPartition(self._partition, constraint=self._dev_nvme.optimalAlignedConstraint)
            self._disk_nvme.commit()

            part_dev = "/dev/"+str(self._partition.getDeviceNodeName())
            logging.debug ("New partition "+part_dev+" created from "+str(free_space[space].start))
            # FIXME: Required to be sure that the partition has effectively been created
            time.sleep (5)
            
        # Create Subsystem
        self._subsystem = nvme.Subsystem(mode='create')
        self._subsystem.set_attr('attr', 'allow_any_host', '1')
        logging.debug ("New NVMe subsystem created: "+self._subsystem.nqn)
                        
        # Create Namespace
        self._namespace = nvme.Namespace(subsystem=self._subsystem, mode='create')
        self._namespace.set_attr('device', 'path', "/dev/"+str(self._partition.getDeviceNodeName()))
        self._namespace.set_enable('1')
        logging.debug ("New NVMe namespace created: "+str(self._namespace._get_nsid())+", path="+part_dev)

        # Create Port and add subsystem
        self._port = nvme.Port (str(current_port_id), mode='any')
        self._port.set_attr('addr', 'trtype', 'rdma')
        self._port.set_attr('addr', 'adrfam', 'ipv4')
        self._port.set_attr('addr', 'traddr', node_ipv4)
        self._port.set_attr('addr', 'trsvcid', str(current_port))
        self._port.add_subsystem (self._subsystem.nqn)
        self._subsys_port = current_port
        logging.debug ("New NVMe Port "+str(current_port_id)+" created: "+node_ipv4+":"+str(current_port)+" (rdma)")
                
        # Required as both the port id and network port have to be unique in a configuration
        current_port_id += 1
        current_port    += 1
                
        return self._subsystem.nqn, self._subsys_port
    
        
    def delete_disk_allocation (self):
        self._port.delete ()
        self._namespace.delete ()
        self._subsystem.delete ()
        self._disk_nvme.deletePartition (self._partition)
        
        self._port      = None
        self._namespace = None
        self._subsystem = None
        self._part_dev  = None
        
        logging.debug ("New NVMe Port created: "+node_ipv4+":4420 (rdma)")
