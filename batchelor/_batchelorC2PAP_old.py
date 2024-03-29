from __future__ import absolute_import

from builtins import str
import multiprocessing
import os
import tempfile
import re

import batchelor
from ._job import JobStatus



_kMemoryUnits = {'mb': 1.0 / 1024.0, 'm': 1.0 / 1024.0, 'gb': 1.0, 'g': 1.0}


def submoduleIdentifier():
	return "c2pap"


def submitJob(config, command, outputFile, jobName, wd = None):

	(fileDescriptor, fileName) = tempfile.mkstemp()
	os.close(fileDescriptor)
	headerFileName = batchelor._getRealPath(config.get(submoduleIdentifier(), "header_file"))
	resources = config.get(submoduleIdentifier(), "resources")
	if config.has_option(submoduleIdentifier(), "memory"):
		if 'ConsumableMemory' in resources:
			resources = re.sub(r"ConsumableMemory\(.*?\)", "", resources).strip()
		resources += " ConsumableMemory({m})".format(m=config.get(submoduleIdentifier(), "memory"))
	with open(fileName, 'w') as tempFile:
		tempFile.write("#!/bin/bash\n\n")
		tempFile.write("#@ group = " + config.get(submoduleIdentifier(), "group") + "\n")
		tempFile.write("#@ output = " + outputFile + "\n")
		tempFile.write("#@ error = " + outputFile + "\n")
		tempFile.write("#@ notification = " + config.get(submoduleIdentifier(), "notification") + "\n")
		tempFile.write("#@ notify_user = " + config.get(submoduleIdentifier(), "notify_user") + "\n")
		tempFile.write("#@ node_usage = " + config.get(submoduleIdentifier(), "node_usage") + "\n")
		tempFile.write("#@ wall_clock_limit = " + config.get(submoduleIdentifier(), "wall_clock_limit") + "\n")
		tempFile.write("#@ resources = " + resources + "\n")
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
		tempFile.write("exec 2>&1\n")
		tempFile.write("\n")
		if wd:
			tempFile.write("cd '{0}'".format(wd))
			tempFile.write("\n\n")
		tempFile.write(command)
	cmnd = "llsubmit - < " + fileName
	(returncode, stdout, stderr) = batchelor.runCommand(cmnd)
	if returncode != 0:
		batchelor.runCommand("rm -f " + fileName)
		raise batchelor.BatchelorException("llsubmit failed (stderr: '" + stderr + "')")
	# example output stdout:
	# llsubmit: The job "mgmt.12309" has been submitted.
	#
	# example output stderr:
	#
	# llsubmit: Stdin job command file written to "/tmp/loadlx_stdin.27558.CdoVxX".
	#
	# INFO: Project: pr83mo
	# INFO: Project's Expiration Date:    2015-01-31
	# INFO: Budget:                     Total [cpuh]        Used [cpuh]      Credit [cpuh]
	# INFO:                                  1350000      1011028 (75%)       338972 (25%)
	#
	# llsubmit: Processed command file through Submit Filter: "/lrz/loadl/filter/submit_filter_c2pap.pl".
	jobId = stdout.split("\n")[0]
	jobId = jobId[jobId.find('"mgmt.')+6:jobId.rfind('"')]
	try:
		jobId = int(jobId)
	except ValueError:
		batchelor.runCommand("rm -f " + fileName)
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

	poolJobsArgs = []
	for job in newJobs:
		poolJobsArgs.append([config] + job)

	pool = multiprocessing.Pool(processes = len(newJobs))
	jobIds = pool.map(_wrapSubmitJob, poolJobsArgs, 1)
	pool.close()
	pool.join()

	return jobIds


def getListOfActiveJobs(jobName):
	return [j.getId() for j in getListOfJobStates(jobName, detailed=False)]


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
	for jobId in jobIds:
		command = "llcancel"
		command += " mgmt." + str(jobId)
		(returncode, stdout, stderr) = batchelor.runCommand(command)
		if returncode != 0:
			raise batchelor.BatchelorException("llcancel failed (stderr: '" + stderr + "')")
	return True



def getListOfJobStates(jobName, username = None, detailed = True):
	if detailed:
		command = "llq -u `whoami` -m -x"
	else:
		command = "llq -u `whoami` -m"
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("llq failed (stderr: '" + stderr + "')")
	jobList = []
	jobStates = []
	currentJobId = -1
	currentJobStatus = None;
	for line in stdout.split('\n'):
		line = line.rstrip('\n')
		if line.startswith("===== Job Step mgmt."):
			try:
				currentJobId = int(line[line.find(".")+1:line.rfind(".")])
				currentJobStatus = JobStatus(currentJobId)
			except ValueError:
				raise batchelor.BatchelorException("parsing of llq output to get job id failed.")
		line = ' '.join(line.split())

		if line.startswith("Job Name: "):
			if currentJobId < 0:
				raise batchelor.BatchelorException("parsing of llq output failed, got job name before job id.")
			name = line[10:]
			if name == jobName or jobName == None:
				jobList.append(currentJobId)
				jobStates.append(currentJobStatus)
		elif line.startswith("Step Virtual Memory: "):
			if currentJobId < 0:
				raise batchelor.BatchelorException("parsing of llq output failed, got job name before job id.")
			try:
				parsed = line.lstrip().lstrip('Step Virtual Memory:').split()
				currentJobStatus.setMemoryUsage( float(parsed[0]) * _kMemoryUnits[parsed[1]], 0)
			except ValueError:
				raise batchelor.BatchelorException("parsing of llq output to get job id failed.")
		elif line.startswith("Status: "):
			if currentJobId < 0:
				raise batchelor.BatchelorException("parsing of llq output failed, got job name before job id.")
			else:
				status = line.lstrip().lstrip("Status: ")
				currentJobStatus.setStatus(JobStatus.kUnknown, name = status)
				if status == 'Running':
					currentJobStatus.setStatus(JobStatus.kRunning)
				elif status == 'I' or status == 'Idle' or status == 'Pending':
					currentJobStatus.setStatus(JobStatus.kWaiting)
				elif status == 'Submission Error' or status == 'Terminated' or status == 'Removed' or status == 'Remove Pending':
					currentJobStatus.setStatus(JobStatus.kError)

		elif line.startswith("Step User Time: "):
			if currentJobId < 0:
				raise batchelor.BatchelorException("parsing of llq output failed, got job name before job id.")
			time_str = line.lstrip().lstrip("Step User Time:").split(':')
			try:
				hours = float(time_str[0])
				minuts = float(time_str[1])
				seconds = float(time_str[2])
				total_time = hours + minuts / 60.0 + seconds / 3600.0
				currentJobStatus.setCpuTime(total_time, 0)
			except ValueError:
				raise batchelor.BatchelorException("parsing of llq output to get job id failed.")
	
	return jobStates
