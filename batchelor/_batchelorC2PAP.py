
import multiprocessing
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
		tempFile.write("#@ notification = " + config.get(submoduleIdentifier(), "notification") + "\n")
		tempFile.write("#@ notify_user = " + config.get(submoduleIdentifier(), "notify_user") + "\n")
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
	cmnd = "llsubmit - < " + fileName
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


def _wrapSubmitJob(args):
	try:
		return submitJob(*args)
	except batchelor.BatchelorException as exc:
		return -1


def submitJobs(config, newJobs):
	if len(newJobs) == 0:
		return []

	for i in range(len(newJobs)):
		newJobs[i].insert(0, config)

	pool = multiprocessing.Pool(processes = len(newJobs))
	jobIds = pool.map(_wrapSubmitJob, newJobs, 1)
	pool.close()
	pool.join()

	return jobIds


def getListOfActiveJobs(jobName):
	if jobName is None:
		command = "llq -u `whoami`"
		(returncode, stdout, stderr) = batchelor.runCommand(command)
		if returncode != 0:
			raise batchelor.BatchelorException("llq failed (stderr: '" + stderr + "')")
		if stdout == "llq: There is currently no job status to report.":
			return []
		stringList = [ job.split()[0] for job in stdout.split('\n')[2:-2] ]
		jobList = []
		try:
			for item in stringList:
				jobId = int(item[item.find(".")+1:item.rfind(".")])
				if jobId not in jobList:
					jobList.append(jobId)
		except ValueError:
			raise batchelor.BatchelorException("parsing of llq output to get job id failed.")
		return jobList
	(fileDescriptor, fileName) = tempfile.mkstemp()
	os.close(fileDescriptor)
	command = "llq -u `whoami` -m &> " + fileName
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("llq failed (stderr: '" + stderr + "')")
	jobList = []
	currentJobId = -1
	with open(fileName, 'r') as llqOutput:
		for line in llqOutput:
			line = line[:-1]
			if line.startswith("===== Job Step xcat."):
				try:
					currentJobId = int(line[line.find(".")+1:line.rfind(".")])
				except ValueError:
					raise batchelor.BatchelorException("parsing of llq output to get job id failed.")
			line = ' '.join(line.split())
			if line.startswith("Job Name: "):
				if currentJobId < 0:
					raise batchelor.BatchelorException("parsing of llq output failed, got job name before job id.")
				name = line[10:]
				if name == jobName:
					jobList.append(currentJobId)
	batchelor.runCommand("rm -f " + fileName)
	return jobList


def getNActiveJobs(jobName):
	return len(getListOfActiveJobs(jobName))


def jobStillRunning(jobId):
	if jobId in getListOfActiveJobs(None):
		return True
	else:
		return False


def getListOfErrorJobs(jobName = None):
	raise batchelor.BatchelorException("not implemented")


def resetErrorJobs(jobName):
	return False


def deleteErrorJobs(jobName):
	return False


def deleteJobs(jobIds):
	if not jobIds:
		return True
	command = "llcancel"
	for jobId in jobIds:
		command += " xcat." + str(jobId)
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("llcancel failed (stderr: '" + stderr + "')")
	return True
