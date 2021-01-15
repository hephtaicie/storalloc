#!/usr/bin/env python3

import yaml

class Node (object):
    """Node class

    Describe a storage node as a remote node with a set of disks
    """
    
    def __init__ (self, idx, node):
        super().__init__()
        
        self._idx      = idx
        self._identity = ''
        self._hostname = node['hostname']
        self._ipv4     = node['ipv4']
        self._bw       = node['network_bw']

        self.disks = list ()

    def get_idx (self):
        return self._idx

    def set_identity (self, identity):
        self._identity = identity

    def get_identity (self):
        return self._identity

    def get_ipv4 (self):
        return self._ipv4

    def get_bw (self):
        return self._bw


class Disk (object):
    """Disk class

    Define a disk with a set of characteristics, including allocated space by jobs
    """
    
    def __init__ (self, idx, disk):
        super().__init__()

        self._idx      = idx

        self._vendor   = disk['vendor']
        self._model    = disk['model']
        self._serial   = disk['serial']
        self._capacity = disk['capacity']
        self._w_bw     = disk['w_bw']
        self._r_bw     = disk['r_bw']
        self._blk_dev  = disk['blk_dev']

        self.allocs    = list ()

    def get_idx (self):
        return self._idx

    def get_bw (self):
        return self._w_bw

    def get_capacity (self):
        return self._capacity

    def get_blk_dev (self):
        return self._blk_dev


class DiskStatus (object):
    """Disk status class

    This object is used temporarily to store the status (bw, occupancy, ...)  of a disk
    while a scheduling algorithm is running
    """
    
    def __init__ (self, idx, capacity):
        super().__init__()
        self._idx     = idx
        self.bw       = 0.0
        self.capacity = capacity

    def get_idx (self):
        return self._idx


class NodeStatus (object):
    """Node status class
    
    This object is used temporarily to store the status (bw, occupancy, ...)  of a node
    while a scheduling algorithm is running
    """

    def __init__ (self, idx):
        super().__init__()
        self._idx        = idx
        self.bw          = 0.0
        self.disk_status = list ()

    def get_idx (self):
        return self._idx

    
class ResourceCatalog (object):
    """ResourceCatalog class

    List of nodes and disks on nodes available for storage allocations. 
    This list can evolve accoding to subscribed storage servers
    """
    
    def __init__ (self):
        super().__init__()
        self._storage_resources = list ()


    @classmethod
    def from_yaml (cls, yaml_file):
        """Translate a system configuration file (YAML) into a set of storage resources."""

        resource_catalog = cls ()
        
        #TODO: file exists?
        stream = open (yaml_file, 'r')
        content = yaml.safe_load (stream)
        stream.close ()

        for idx_n, node in enumerate(content['hosts']):
            new_node = Node (idx_n, node)
            for idx_d, disk in enumerate (node['disks']):
                new_disk = Disk (idx_d, disk)
                new_node.disks.append (new_disk)
            resource_catalog._storage_resources.append (new_node)
        
        return resource_catalog
        

    def add_allocation (self, node, disk, request):
        self._storage_resources[node].disks[disk].allocs.append (request)
        
    
    def get_resources_list (self):
        return self._storage_resources


    def get_identity_of_node (self, node):
        return self._storage_resources[node].get_identity()
    
        
    def is_empty (self):
        if not self._storage_resources:
            return True
        return False

        
    def append_resources (self, src_identity, resources):
        """Append the given resources received from a server to the catalog."""

        for node in resources:
            node.set_identity (src_identity)
            self._storage_resources.append (node)

        

    def print_status (self, target_node_id, target_disk_id):
        """ASCII-based output of the given scheduling."""
        
        # Concatenate lists of requests per disk to determine ealiest start time and latest end time
        all_requests_list = list ()
        for n in range (0, len(self._storage_resources)):
            for d in range (0, len(self._storage_resources[n].disks)):
                all_requests_list.extend (self._storage_resources[n].disks[d].allocs)

        if all_requests_list:
            earliest_request = min([x.get_start_time() for x in all_requests_list])
            latest_request   = max([x.get_end_time() for x in all_requests_list])
            steps            = int((latest_request - earliest_request).seconds / 300) # granularity: 5 minutes
        else:
            earliest_request = 0
            latest_request   = 0
            steps            = 0

        # Print the current status of the scheduling on nodes and disks
        for n in range (0, len(self._storage_resources)):
            print ("┌───┬", end ="")
            for s in range (0, steps):
                print ("─", end ="")
            print ()
            for d in range (0, len(self._storage_resources[n].disks)):
                if not self._storage_resources[n].disks[d].allocs:
                    print ("│"+str(d).rjust(3)+"│")
                else:
                    for i, r in enumerate(self._storage_resources[n].disks[d].allocs):
                        if i == 0:
                            print ("│"+str(d).rjust(3)+"│", end="")
                        else:
                            print ("│   │", end="")
                        offset = int((r.get_start_time() - earliest_request).seconds / 300)
                        for o in range (0, offset):
                            print (" ", end="")
                        req_time = int((r.get_end_time() - r.get_start_time ()).seconds / 300)
                        req_time = 1 if req_time == 0 else req_time
                        for j in range (0, req_time):
                            if target_node_id == n and target_disk_id == d and i == len(self._storage_resources[n].disks[d].allocs) - 1:
                                print ("□", end="")
                            else:
                                print ("■", end="")
                        print ()
                if d < len(self._storage_resources[n].disks) - 1:
                    print ("├---┼", end="")
                    for s in range (0, steps):
                        print ("-", end ="")
                    print()
            print ("└───┴", end ="")
            for s in range (0, steps):
                print ("─", end ="")
            print ()
