
import ConfigParser
import os
import tempfile
import random
import string

import batchelor


def submoduleIdentifier():
	return "lxplus"


def submitJob(config, command, outputFile, jobName, arrayStart = None, arrayEnd = None, arrayStep = None):
	(fileDescriptor, fileName) = tempfile.mkstemp()
	os.close(fileDescriptor)
	batchelor.runCommand("cp " + batchelor._getRealPath(config.get(submoduleIdentifier(), "header_file")) + " " + fileName)
	with open(fileName, 'a') as scriptFile:
		scriptFile.write(command)
	if arrayStart is not None:
		if (jobName is None) or (len(jobName) is 0):
			jobName = ''.join(random.sample(string.lowercase,7))
		jobName = jobName + "[" + str(arrayStart) + "-" +  str(arrayEnd) + ":" + str(arrayStep) + "]"
	cmnd = "bsub "
	cmnd += "" if jobName is None else ("-J " + jobName + " ")
	cmnd += "-o " + outputFile + " "
	cmnd += "-q " + config.get(submoduleIdentifier(), "queue") + " "
	cmnd += "-R '"
	cmnd += " select[type=" + config.get(submoduleIdentifier(), "type") + "]"
	cmnd += " rusage[pool=" + config.get(submoduleIdentifier(), "pool") + "]"
	try:
		cmnd += " rusage[mem=" + config.get(submoduleIdentifier(), "memory") + "]"
		cmnd += " select[maxmem>" + config.get(submoduleIdentifier(), "memory") + "]"
	except ConfigParser.NoOptionError:
		pass
	cmnd += _getExcludedHostsString(config)
	cmnd += "' "
	cmnd += "< " + fileName
	(returncode, stdout, stderr) = batchelor.runCommand(cmnd)
	if returncode != 0:
		raise batchelor.BatchelorException("bsub failed (stderr: '" + stderr + "')")
# example output: Job <533476534> is submitted to queue <1nd>.
	jobId = stdout.lstrip("Job <")
	jobId = jobId[:jobId.find(">")]
	try:
		jobId = int(jobId)
	except ValueError:
		raise batchelor.BatchelorException('parsing of bsub output to get job id failed.')
	batchelor.runCommand('rm -f ' + fileName)
	return jobId


def getExtendedListOfActiveJobs(jobName):
	command = "bjobs"
	if not jobName is None:
		command = command + " -J " + jobName
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	#example output:
	#JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
	#561084149 hubers  PEND  1nd        lxplus0158.             *-J k[1-3] Sep  3 15:50
	if returncode != 0:
		raise batchelor.BatchelorException("bjobs failed (stderr: '" + stderr + "')")
	if stdout == "":
		return []
	jobList = stdout.split('\n')[1:]
	try:
		return [ (int(job.split()[0]), job.split()[-4][job.split()[-4].find("[")+1:-1] if job.split()[-4].find("[") != -1 else '', job.split()[2] ) for job in jobList ]
		#example output: [(jobId, taskId, jobStatus), ...]
	except ValueError:
		raise batchelor.BatchelorException("parsing of bjobs output to get job id failed.")


def getListOfActiveJobs(jobName):
	returnList = []
	for job in getExtendedListOfActiveJobs(jobName):
		returnList.append(job[0])
	return returnList


def getNActiveJobs(jobName):
	return len(getListOfActiveJobs(jobName))


def jobStillRunning(jobId):
	if jobId in  getListOfActiveJobs(None):
		return True
	else:
		return False


def deleteJobs(jobIds):
	if not jobIds:
		return True
	command = "bkill"
	for jobId in jobIds:
		# deleteJobs might be called with two different kind of arguments:
		# Being internally called, it may get a list of tuples from, e.g.,
		# the getExtendedListOfActiveJobss function. On the otherhand, when called
		# from the outside a simple list of job IDs (ints) might be passed.
		if type(jobId) is tuple:
			if len(jobId) > 1 and jobId[1] != "":
				command += " " + str(jobId[0]) + "[" + str(jobId[1]) + "]"
			else:
				command += " " + str(jobId[0])
		else:
			command += " " + str(jobId)
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		if not 'Job has already finished' in stderr:
			raise batchelor.BatchelorException("bkill failed (stderr: '" + stderr + "')")
	return True


def _getExcludedHostsString(config):
	try:
		hosts = config.get(submoduleIdentifier(),"excluded_hosts").split()
	except ConfigParser.NoOptionError:
		return ''
	excludedString = ' select['
	for host in hosts:
		if not excludedString == ' select[':
			excludedString = excludedString + '&&'
		excludedString = excludedString + '(hname!=' + host + ')'
	excludedString = excludedString + ']'
	return excludedString
