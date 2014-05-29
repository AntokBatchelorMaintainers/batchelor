
import os
import xml.etree.ElementTree as ElementTree

import batchelor
import tempfile


def submoduleIdentifier():
	return "gridka"


def submitJob(config, command, outputFile, jobName):
	(fileDescriptor, fileName) = tempfile.mkstemp()
	os.close(fileDescriptor)
	batchelor.runCommand("cp " + batchelor._getRealPath(config.get(submoduleIdentifier(), "header_file")) + " " + fileName)
	with open(fileName, 'a') as scriptFile:
		scriptFile.write(command)
	cmnd = "qsub "
	cmnd += "-j y -b y "
	cmnd += "" if jobName is None else ("-N " + jobName + " ")
	cmnd += "-o " + outputFile + " "
	cmnd += "-P " + config.get(submoduleIdentifier(), "project") + " "
	cmnd += "-q " + config.get(submoduleIdentifier(), "queue") + " "
	cmnd += "-l h_vmem=" + config.get(submoduleIdentifier(), "memory") + " "
	cmnd += "< " + fileName
	(returncode, stdout, stderr) = batchelor.runCommand(cmnd)
	if returncode != 0:
		raise batchelor.BatchelorException("qsub failed")
	# example output: "Your job 1601905 ("J2415c980b8") has been submitted"
	jobId = stdout.lstrip("Your job ")
	jobId = jobId[:jobId.find(' ')]
	try:
		jobId = int(jobId)
	except ValueError:
		raise batchelor.BatchelorException('parsing of qsub output to get job id failed.')
	batchelor.runCommand('rm -f ' + fileName)
	return jobId


def getNJobs(jobName):
	if jobName is None:
		raise batchelor.BatchelorException("Not implemented")
	command = "qstat -j " + jobName
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		if stderr and stderr.split('\n')[0][:-1] == 'Following jobs do not exist or permissions are not sufficient:':
			return 0
		raise batchelor.BatchelorException("qstat failed")
	command = "qstat -xml -j " + jobName
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if stdout == "" and stderr == "":
		return 0
	root = ElementTree.fromstring(stdout)
	nJobs = 0
	for child in root[0]:
		nJobs += 1
	return(nJobs)


def jobStillRunning(jobId):
	jobId = str(jobId)
	if getNJobs(jobId) == 1:
		return True
	else:
		return False
