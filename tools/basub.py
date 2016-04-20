#!/usr/bin/env python
# coding: utf-8
'''
Created on Apr 20, 2016

@author: ga34liw
'''

program_description = ''' 
    Submits the given command via batcherlor
'''

# std includes
import argparse
import os
import re

# root includes

# own includes
import batchelor



def main():
    argparser = argparse.ArgumentParser( )
    argparser.add_argument('--batchelorConfigfile', default="~/.batchelorrc",help="Path to bachelor configuration file [default: %(default)s].")
    argparser.add_argument('--batchelorVerbose', action='store_true', help="Increase output verbosity.")
    argparser.add_argument('--batchelorLocal', action='store_true', help="Run locally.")
    argparser.add_argument('--batchelorMemory', default=None, help="Override memory consumption from batchelor configuration file (e.g. 2G).")
    argparser.add_argument('--batchelorJobName', default="BaSub", help="Set job name [default: %(default)s].")
    argparser.add_argument('--batchelorFor', action="append", help="Submits one job for each element given by batchelorFor (e.g. --batchelorFor1 --batchelorFor2). Thereby BAFOR in the command string is substituted by the current element.")
    argparser.add_argument('--batchelorForMinMax', help="Submits one job for each element in the range [i_min,i_max] (e.g. --batchelorForMinMax 1,4). Thereby BAFOR in the command string is substituted by the current element.")
#     optparser = OptionParser( usage="Usage:%prog [<options>] <command incl options>", description = program_description );
#     optparser.add_option('', '--n-threads', dest='n_threads', action='store', type='int', default=multiprocessing.cpu_count(), help="Number of parallel threads for [default: %default].")

    
    args, commandArgs = argparser.parse_known_args()
    
    
    batchelorConfigfile = os.path.expanduser(args.batchelorConfigfile);
    

    batchelorFor = []
    if args.batchelorFor:
        batchelorFor = args.batchelorFor;
    
    if args.batchelorForMinMax:
        parsed = re.findall('([0-9]+),([0-9]+)', args.batchelorForMinMax)
        if not parsed:
            print "Can not parse --batchelorForMinMax flag '{0}'".format( parsed )
            exit(1)
        parsed = parsed[0]
        batchelorFor += map( str, range( int(parsed[0]), int(parsed[1]) + 1 ) )
    
    
    if not commandArgs:
        print "No command given"
        exit(1)
    
    command = ' '.join(commandArgs) 

    handler = batchelor.BatchelorHandler( batchelorConfigfile, 
                                          systemOverride = "" if not args.batchelorLocal else "local", 
                                          memory = 0 if not args.batchelorMemory else args.batchelorMemory, 
                                          check_job_success = True
                                        )
    
    if not batchelorFor:
        handler.submitJob(command, jobName = args.batchelorJobName)
        if args.batchelorVerbose:
            print "Execute command:\n\t'{0}'".format(command)
    else:
        for e in batchelorFor:
            command_local = command.replace("BAFOR", e)
            handler.submitJob(command_local, jobName = args.batchelorJobName)
            if args.batchelorVerbose:
                print "Execute command:\n\t'{0}'".format(command_local)
    
    handler.wait()
    st= handler.checkJobStates() 
    handler.shutdown()

    

    exit(0 if st else 1)


if __name__ == '__main__':
    main()