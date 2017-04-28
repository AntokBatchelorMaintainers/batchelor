#!/usr/bin/env python
# coding: utf-8
'''
Created on Wed Apr 26 16:48:05 2017


@author: Stefan Wallner
'''

# a few useful default inputs
import os,sys
import subprocess as sp
import shutil
import numpy as np

import batchelor



configfile = "~/.ibatchelorrc" if os.path.isfile(os.path.expanduser("~/.ibatchelorrc")) else "~/.batchelorrc"

if '--local' in  sys.argv:
	sys = 'local'
else:
	sys = ''

bh = batchelor.BatchelorHandler(configfile=configfile, systemOverride=sys, check_job_success=True)

