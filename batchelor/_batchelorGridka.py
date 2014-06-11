
import ConfigParser
import glob
import os
import tempfile
import xml.etree.ElementTree as ElementTree

import batchelor


def submoduleIdentifier():
	return "gridka"


def submitJob(config, command, outputFile, jobName, arrayStart = None, arrayEnd = None, arrayStep = None):
	(fileDescriptor, fileName) = tempfile.mkstemp()
	os.close(fileDescriptor)
	batchelor.runCommand("cp " + batchelor._getRealPath(config.get(submoduleIdentifier(), "header_file")) + " " + fileName)
	with open(fileName, 'a') as scriptFile:
		scriptFile.write(command)
	cmnd = "qsub "
	cmnd += "-j y "
	cmnd += "" if jobName is None else ("-N " + jobName + " ")
	if arrayStart is not None:
		cmnd += "-t " + str(arrayStart) + "-" + str(arrayEnd) + ":" + str(arrayStep) + " "
	cmnd += "-o " + outputFile + " "
	cmnd += "-P " + config.get(submoduleIdentifier(), "project") + " "
	cmnd += "-q " + config.get(submoduleIdentifier(), "queue") + " "
	cmnd += "-l h_vmem=" + config.get(submoduleIdentifier(), "memory") + " "
	cmnd += _getExcludedHostsString(config)
	cmnd += "< " + fileName
	(returncode, stdout, stderr) = batchelor.runCommand(cmnd)
	if returncode != 0:
		raise batchelor.BatchelorException("qsub failed (stderr: '" + stderr + "')")
	# example output: "Your job 1601905 ("J2415c980b8") has been submitted"
	if arrayStart is not None:
		jobId = stdout.lstrip("Your job-array ")
		jobId = jobId[:jobId.find('.')]
	else:
		jobId = stdout.lstrip("Your job ")
		jobId = jobId[:jobId.find(' ')]
	try:
		jobId = int(jobId)
	except ValueError:
		raise batchelor.BatchelorException('parsing of qsub output to get job id failed.')
	batchelor.runCommand("rm -f " + fileName)
	return jobId


def getListOfActiveJobs(jobName):
	command = "qstat"
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("qstat failed (stderr: '" + stderr + "')")
	jobList = stdout.split('\n')[2:]
	if len(jobList) is 0:
		return []
	if jobName is not None:
		possibleJobIds = []
		for job in jobList:
			if job.split()[2][:10] != jobName[:10]:
				continue
			if job.split()[0] not in possibleJobIds:
				possibleJobIds.append(job.split()[0])
		command = "qstat -j " + ','.join(possibleJobIds) + " |grep 'job_number\|job_name'"
		(returncode, stdout, stderr) = batchelor.runCommand(command)
		if returncode != 0:
			raise batchelor.BatchelorException("qstat failed (stderr: '" + stderr + "')")
		if stdout == "":
			return []
		jobIdList = stdout.split('\n')
		possibleJobIds = []
		for i in range(0,len(jobIdList),2):
			if jobIdList[i+1].split()[1] != jobName:
				continue
			possibleJobIds.append(jobIdList[i].split()[1])
	returnList = []
	try:
		for job in jobList:
			if jobName is None or job.split()[0] in possibleJobIds:
				returnList.append( ( int(job.split()[0]), job.split()[-1] if job.split()[-2].isdigit() else '', job.split()[4] ) )
		return returnList
	except ValueError:
		raise batchelor.BatchelorException("parsing of qstat output to get job id failed.")

def getNActiveJobs(jobName):
	Njobs = 0
	for job in getListOfActiveJobs(jobName):
		for taskGroup in job[1].split(','):
			if len(taskGroup.split('-')) is 1:
				Njobs += 1
			else:
				distance = int(taskGroup.split('-')[1].split(':')[0]) -  int(taskGroup.split('-')[0]) + 1
				Njobs += int( (distance / float(taskGroup.split('-')[1].split(':')[1])) + 0.5 )
	return Njobs


def jobStillRunning(jobId):
	if jobId in getListOfActiveJobs(str(jobId)):
		return True
	else:
		return False


def getListOfErrorJobs(jobName):
	listOfActiveJobs = getListOfActiveJobs(jobName)
	listOfErrorJobs = []
	for job in listOfActiveJobs:
		if job[1] == "Eqw":
			listOfErrorJobs.append(job)
	return listOfErrorJobs


def resetErrorJobs(jobName):
	return False


def deleteErrorJobs(jobName):
	return deleteJobs(getListOfErrorJobs(jobName))


def deleteJobs(jobIds):
	if not jobIds:
		return True
	command = "qdel"
	for jobId in jobIds:
		if len(jobId) > 1 and jobId[1] != "":
			for taskGroup in jobId[1].split(','):
				command += " " + str(jobId[0]) + " -t " + taskGroup
		else:
			command += " " + str(jobId[0]) if type(jobId) is tuple else " " + str(jobId)
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("qdel failed (stderr: '" + stderr + "')")
	return True


def _getExcludedHostsString(config):
	try:
		hosts = config.get(submoduleIdentifier(),"excluded_hosts").split()
	except ConfigParser.NoOptionError:
		return ''
	excludedString = "-l 'hostname="
	for host in hosts:
		if not excludedString == "-l 'hostname=":
			excludedString = excludedString + "&"
		excludedString = excludedString + "!" + host
	excludedString = excludedString + "' "
	return excludedString
