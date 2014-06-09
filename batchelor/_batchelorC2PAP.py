
import os
import tempfile

import batchelor


def submoduleIdentifier():
	return "c2pap"


def submitJob(config, command, outputFile, jobName):
	(fileDescriptor, fileName) = tempfile.mkstemp()
	os.close(fileDescriptor)
	headerFileName = batchelor._getRealPath(config.get(submoduleIdentifier(), "header_file"))
	with open(fileName, 'w') as tempFile:
		tempFile.write("#!/bin/bash\n\n")
		tempFile.write("#@ group = " + config.get(submoduleIdentifier(), "group") + "\n")
		tempFile.write("#@ output = " + outputFile + "\n")
		tempFile.write("#@ node_usage = " + config.get(submoduleIdentifier(), "node_usage") + "\n")
		tempFile.write("#@ wall_clock_limit = " + config.get(submoduleIdentifier(), "wall_clock_limit") + "\n")
		tempFile.write("#@ resources = " + config.get(submoduleIdentifier(), "resources") + "\n")
		tempFile.write("#@ job_type = " + config.get(submoduleIdentifier(), "job_type") + "\n")
		tempFile.write("#@ class = " + config.get(submoduleIdentifier(), "job_type") + "\n")
		if jobName is not None:
			tempFile.write("#@ job_name = " + jobName + "\n")
		tempFile.write("#@ queue\n\n\n")
		with open(headerFileName, 'r') as headerFile:
			for line in headerFile:
				if line.startswith("#!"):
					continue
				tempFile.write(line)
		tempFile.write("\n\n")
		tempFile.write(command)
	cmnd = "llsubmit " + fileName
	(returncode, stdout, stderr) = batchelor.runCommand(cmnd)
	if returncode != 0:
		raise batchelor.BatchelorException("llsubmit failed (stderr: '" + stderr + "')")
	# example ouptut:
	#
	#INFO: Project: pr83mo
	#INFO: Project's Expiration Date:    2015-01-31
	#INFO: Budget:                     Total [cpuh]        Used [cpuh]      Credit [cpuh]
	#INFO:                                  1350000       259009 (19%)      1090991 (81%)
	#
	#llsubmit: Processed command file through Submit Filter: "/lrz/loadl/filter/submit_filter_c2pap.pl".
	#llsubmit: The job "xcat.2021566" has been submitted.
	jobId = stdout.split("\n")[-1]
	jobId = jobId[jobId.find('"xcat.')+6:jobId.rfind('"')]
	try:
		jobId = int(jobId)
	except ValueError:
		raise batchelor.BatchelorException('parsing of qsub output to get job id failed.')
	batchelor.runCommand("rm -f " + fileName)
	return jobId


def getListOfActiveJobs(jobName):
	raise batchelor.BatchelorException("not implemented")


def getNActiveJobs(jobName):
	raise batchelor.BatchelorException("not implemented")


def jobStillRunning(jobId):
	raise batchelor.BatchelorException("not implemented")


def getListOfErrorJobs(jobName = None):
	raise batchelor.BatchelorException("not implemented")


def resetErrorJobs():
	raise batchelor.BatchelorException("not implemented")


def deleteErrorJobs(jobName):
	raise batchelor.BatchelorException("not implemented")


def deleteJobs(jobIds):
	raise batchelor.BatchelorException("not implemented")
