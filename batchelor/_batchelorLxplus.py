
import batchelor


def submoduleIdentifier():
	return "lxplus"


def submitJob(*posArgs, **keywords):
	raise batchelor.BatchelorException("Not implemented")


def getNJobs(jobName):
	raise batchelor.BatchelorException("Not implemented")


def jobStillRunning(jobId):
	raise batchelor.BatchelorException("Not implemented")
