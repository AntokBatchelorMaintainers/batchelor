#!/usr/bin/env python

from __future__ import print_function
from builtins import str
import batchelor
import sys

batch = batchelor.Batchelor()
batch.debug = True

print("initialized = " + str(batch.initialized()))

if not batch.initialize("example.config"):
	print("initialization failed")
	sys.exit(1)

print("initialized = " + str(batch.initialized()))

print("n jobs: " + str(batch.getNActiveJobs("J6ac89b30be")))
