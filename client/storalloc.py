#!/usr/bin/env python3

import argparse
import logging
import socket
import sys
import os
from shutil import which

sys.path.append("..")
from common.status import StatusFile

# Default values
SERV = '148.187.104.85'
PORT = 6666
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
status_file = '/users/ftessier/client_status.yml'

req_size   = 0
req_time   = 0

def parse_args ():
    global req_size, req_time

    parser = argparse.ArgumentParser ()
    parser.add_argument ('-s', '--size', type=int, help="Size of the requested storage allocation (GB)")
    parser.add_argument ('-t', '--time', type=int, help="Total run time of the storage allocation (min)")
    parser.add_argument ('-d', '--disconnect', type=int, help="Disconnect previously attached NVMe storage targets")
    parser.add_argument ('-v', '--verbose', help="Display debug information", action='store_true')
    
    args = parser.parse_args ()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")

    if not args.size or args.size == 0:
        parser.print_usage()
        print ('Error: argument --size (-s) is mandatory!')
        sys.exit(1)
    else:
        req_size = args.size

    if not args.time or args.time == 0:
        parser.print_usage()
        print ('Error: argument --time (-t) is mandatory!')
        sys.exit(1)
    else:
        req_time = args.time
    

def build_request (size, time, host, host_ip):
    return bytearray(str(size)+', '+str(time), 'utf-8')


def main (argv):
    global sock, req_size, req_time

    parse_args ()

    host    = socket.gethostname()
    host_ip = socket.gethostbyname(host)

    request = build_request (req_size, req_time, host, host_ip)
    
    sock.connect((SERV, PORT))
    logging.debug ('Submitting request ['+request.decode('utf-8')+'] from '+str(host)+':'+str(host_ip))
    sock.send(request)

    while True:
        try:
            data = sock.recv(1024)
            data_str = data.decode('utf-8')
            if data_str.startswith ('nqn'):
                nqn       = data_str.split(' ')[1]
                nvme_port = data_str.split(' ')[2]
                logging.debug ('Connecting remote storage target '+nqn+' via port '+nvme_port)
                if which('nvme') is not None:
                    cmd = os.popen('sudo nvme connect -a '+SERV+' -t rdma -s '+nvme_port+' -n '+nqn)
                    # output = cmd.read()
                else:
                    print ('Error: nvme tool does not exist. Unable to connect the remote storage target!')
                    sys.exit(1)
            elif data_str == "EOT" or not data_str:
                break
            else:
                print ("storalloc: "+data.decode('utf-8'))
        except socket.error:
            print ('Error: Connection lost!')
            sys.exit(1)
            

    sock.close()



if __name__ == "__main__":
    main (sys.argv[1:])
