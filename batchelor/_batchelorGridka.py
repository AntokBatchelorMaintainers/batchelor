
import xml.etree.ElementTree as ElementTree

import batchelor


def submoduleIdentifier():
	return "gridka"


def submitJob(**keywords):
	raise batchelor.BatchelorException("Not implemented")


def getNoRunningJobs(jobName):
	command = "qstat -j " + jobName
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		if stderr and stderr.split('\n')[0][:-1] == 'Following jobs do not exist or permissions are not sufficient:':
			return 0
# TODO: Think of a correct treatment of this case, i.e. the qstat command returned
#       a return code != 0 but it is a different problem than the job not existing.
#       The current behavior was introduced because this piece of code was
#       originally introduced to check if the number of jobs fell below a certain
#       threshold and it made sense to return a large number in case qstat failed.
#       Probably, one should raise an exception here, but that means the user has
#       to build a try/catch block around every call to this function because qstat
#       may fail often. Maybe there is a better solution (return a negative
#       number?)...
		return 9999
	command = "qstat -xml -j " + jobName
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if stdout == "" and stderr == "":
		return 0
	root = ElementTree.fromstring(stdout)
	nJobs = 0
	for child in root[0]:
		nJobs += 1
	return(nJobs)
