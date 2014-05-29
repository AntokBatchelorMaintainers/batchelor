
import batchelor


def submoduleIdentifier():
	return "e18"


def submitJob(*posArgs, **keywords):
	raise batchelor.BatchelorException("Not implemented")


def getNJobs(jobName):
	raise batchelor.BatchelorException("Not implemented")


def jobStillRunning(jobId):
	raise batchelor.BatchelorException("Not implemented")
