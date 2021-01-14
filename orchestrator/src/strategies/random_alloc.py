#!/usr/bin/env python3

class RandomAlloc (object):

    def __init__ (self):
        super().__init__()

        
    def compute (self, storage_resources, new_r):
        random.seed()
        target_node = randint(0, 1)
        target_disk = randint(0, 9)

        return target_node, target_disk
