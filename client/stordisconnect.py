#!/usr/bin/env python3

import argparse
import logging
import sys
import os
from shutil import which

def main (argv):
    
    parser = argparse.ArgumentParser ()
    parser.add_argument ('-d', '--device', help="Disconnect the device (/dev/nvme[..]) given as parameter")
    parser.add_argument ('-a', '--all', help="Disconnect all the previously attached NVMe storage targets", action='store_true')
    parser.add_argument ('-v', '--verbose', help="Display debug information", action='store_true')
    
    args = parser.parse_args ()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")

    if not args.all and not args.device:
        parser.print_usage()
        print ('Error: At least one arguments is required!')
        sys.exit(1)

    if args.all and args.device:
        parser.print_usage()
        print ('Error: Arguments -a (--all) and -d (--device) are exclusive!')
        sys.exit(1)
    

    if which('nvme') is not None:
        if args.all:
            print ("all")
        else:
            cmd = os.popen('sudo nvme disconnect -d '+args.device)
            output = cmd.read()
            print (output)
            logging.debug ("Disconnecting "+args.device)
    else:
        print ('Error: nvme tool does not exist. Unable to connect the remote storage target!')
        sys.exit(1)
        

if __name__ == "__main__":
    main (sys.argv[1:])
