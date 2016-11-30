
import datetime


jobs = []
highestId = 0


class Job:

	def __init__(self, jobId, command, outputFile, jobName, jobFinishTime):
		self.jobId = jobId
		self.command = command
		self.outputFile = outputFile
		self.jobName = jobName
		self.jobFinishTime = jobFinishTime

	def __str__(self):
		retval = "Simulated job " + self.jobId
		retval += ": command '" + self.command + "', "
		retval += "will finish at " + str(jobFinishTime)
		return retval


def submoduleIdentifier():
	return "simulator"


def submitJob(config, command, outputFile, jobName, wd=None):
	if wd:
		raise batchelor.BatchelorException("Choosing the working directory is not jet implemented for {0}".format(submoduleIdentifier()))

	lifetimeString = config.get(submoduleIdentifier(), "lifetime")
	lifetime = datetime.datetime.strptime(lifetimeString, "%H:%M:%S") - datetime.datetime(1900, 1, 1, 0, 0, 0)
	jobFinishTime = datetime.datetime.now() + lifetime
	global highestId
	jobId = highestId + 1
	highestId += 1
	jobs.append(Job(jobId, command, outputFile, jobName, jobFinishTime))
	return jobId


def getListOfActiveJobs(jobName):
	jobsToRemove = []
	for job in jobs:
		if job.jobFinishTime < datetime.datetime.now():
			jobsToRemove.append(job)
	for job in jobsToRemove:
		jobs.remove(job)
	relevantJobs = []
	if jobName is None:
		relevantJobs = jobs
	else:
		for job in jobs:
			if job.jobName == jobName:
				relevantJobs.append(job)
	return [ job.jobId for job in relevantJobs ]


def getNActiveJobs(jobName):
	return len(getListOfActiveJobs(jobName))


def jobStillRunning(jobId):
	return jobId in getListOfActiveJobs(None)


def getListOfErrorJobs(jobName):
	return []


def resetErrorJobs(jobName):
	return True


def deleteErrorJobs(jobName):
	return True


def deleteJobs(jobIds):
	jobsToRemove = []
	for job in jobs:
		if job.jobId in jobIds:
			jobsToRemove.append(job)
	for job in jobsToRemove:
		jobs.remove(job)
	return True
