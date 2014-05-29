
import xml.etree.ElementTree as ElementTree

import batchelor


def submoduleIdentifier():
	return "gridka"


def submitJob(**keywords):
	raise batchelor.BatchelorException("Not implemented")


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
