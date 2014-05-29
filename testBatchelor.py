#!/usr/bin/env python

import batchelor

batch = batchelor.Batchelor()
batch.debug = True

print("initialized = " + str(batch.initialized()))

batch.initialize("example.config")

print("initialized = " + str(batch.initialized()))

print("n jobs: " + str(batch.getNJobs("J6ac89b30be")))
