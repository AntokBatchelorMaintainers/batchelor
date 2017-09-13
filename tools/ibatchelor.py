#!/usr/bin/env python
# coding: utf-8
'''
Created on Wed Apr 26 16:48:05 2017


@author: Stefan Wallner
'''


program_description = '''
Interactive python console to submit jobs using batchelor.
'''

# a few useful default imports
import os,sys
import subprocess as sp
import shutil
import numpy as np
from optparse import OptionParser

import batchelor


configfile = "~/.ibatchelorrc" if os.path.isfile(os.path.expanduser("~/.ibatchelorrc")) else "~/.batchelorrc"

optparser = OptionParser( usage="Usage:ibatchelor [<options>]", description = program_description );
optparser.add_option('-l', '--local', action='store_true', help="Execute jobs on the local machine.")
optparser.add_option('-m', '--memory', action='store', default=None, type='str', help="Mempory reservation for the jobs. Taken from {0} if not given.".format(configfile))

( options, args ) = optparser.parse_args();

if options.local:
	sys = 'local'
else:
	sys = ''

if options.memory != None:
    mem = options.memory
else:
    mem = None

bh = batchelor.BatchelorHandler(configfile=configfile, systemOverride=sys, memory=mem, check_job_success=True)

