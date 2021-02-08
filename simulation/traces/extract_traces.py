#!/usr/bin/env python3

import sys
import argparse
import csv

data_file = None

def parse_args ():
    global data_file

    parser = argparse.ArgumentParser ()
    parser.add_argument ('-d', '--data', help="Path of the Darshan data file")
    parser.add_argument ('-v', '--verbose', help="Display debug information", action='store_true')

    args = parser.parse_args ()

    if not args.data:
        parser.print_usage()
        print ('Error: argument --data (-d) is mandatory!')
        sys.exit(1)
    else:
        data_file = args.data

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")


def main (argv):
    parse_args ()
    with open(data_file, newline='') as data:
        traces = csv.reader(data, delimiter=',')

        #next (trace_lines)
        for i, row in enumerate(traces):
            print (row)
    

if __name__ == "__main__":
    main (sys.argv[1:])
