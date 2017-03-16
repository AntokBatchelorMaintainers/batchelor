
import multiprocessing
import os
import sys
import tempfile
import time

import batchelor
from _job import JobStatus




def submoduleIdentifier():
	return "lrz"


def submitJob(config, command, outputFile, jobName, wd = None):
	
	
	# check if only a certain amount of active jobs is allowd
	if config.has_option(submoduleIdentifier(), "max_active_jobs"):
		max_active_jobs = int(config.get(submoduleIdentifier(), "max_active_jobs"))
		i=0;
		waitTime = 90
		while True:
			try:
				nRunningJobs = len(getListOfActiveJobs(None))
			except batchelorn.BatchelorException:
				nRunningJobs = max_active_jobs
			if nRunningJobs < max_active_jobs:
				break
			if i == 0:
				sys.stdout.write("Waiting for free slots")
				sys.stdout.flush()
			time.sleep(waitTime); # wait 1.5  min
			i+=1
		if i > 0:
			sys.stdout.write("\r")

	if wd == None:
		wd = os.getcwd()
	(fileDescriptor, fileName) = tempfile.mkstemp()
	os.close(fileDescriptor)
	headerFileName = batchelor._getRealPath(config.get(submoduleIdentifier(), "header_file"))
	with open(fileName, 'w') as tempFile:
		tempFile.write("#!/bin/bash\n\n")
		tempFile.write("#SBATCH -D " + wd + "\n")
		tempFile.write("#SBATCH -o " + outputFile + "\n")
		tempFile.write("#SBATCH --time=" + config.get(submoduleIdentifier(), "wall_clock_limit") + "\n")
		tempFile.write("#SBATCH --mem=" + config.get(submoduleIdentifier(), "memory") + "\n")
		if jobName is not None:
			tempFile.write("#SBATCH -J " + jobName + "\n")
		tempFile.write("#SBATCH --get-user-env \n")
		tempFile.write("#SBATCH --export=NONE \n")
		tempFile.write("#SBATCH --clusters=serial \n\n\n")
		with open(headerFileName, 'r') as headerFile:
			for line in headerFile:
				if line.startswith("#!"):
					continue
				tempFile.write(line)
		tempFile.write("\n\n")
		tempFile.write(command)
	cmnd = "sbatch " + fileName
	(returncode, stdout, stderr) = batchelor.runCommand(cmnd)
	batchelor.runCommand("rm -f " + fileName)
	if returncode != 0:
		raise batchelor.BatchelorException("sbatch failed (stderr: '" + stderr + "')")
	jobId = stdout.split()[3]
	try:
		jobId = int(jobId)
	except ValueError:
		raise batchelor.BatchelorException('parsing output of sbatch to get job id failed.')
	return jobId


def _wrapSubmitJob(args):
	try:
		return submitJob(*args)
	except batchelor.BatchelorException as exc:
		return -1


def getListOfActiveJobs(jobName):
	return map( lambda j: j.getId(), getListOfJobStates(jobName, detailed=False) )


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
	command = "scancel --clusters=serial"
	for jobId in jobIds:
		command += " " + str(jobId)
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("scancel failed (stderr: '" + stderr + "')")
	return True



def getListOfJobStates(jobName, username = None, detailed = True):
	command = "squeue --clusters=serial -u $(whoami) -l -h"
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("squeue failed (stderr: '" + stderr + "')")
	jobList = []
	jobStates = []
	for line in stdout.split('\n'):
		if line.startswith("CLUSTER: serial"):
			continue;
		line = line.rstrip('\n')
		lineSplit = line.split()
		try:
			currentJobId = int(lineSplit[0])
			currentJobStatus = JobStatus(currentJobId)

			# name
			name = lineSplit[2]
			if name == jobName or jobName == None:
				jobList.append(currentJobId)
				jobStates.append(currentJobStatus)

			# status
			status = lineSplit[4]
			currentJobStatus.setStatus(JobStatus.kUnknown, name = status)
			if status=='RUNNING':
				currentJobStatus.setStatus(JobStatus.kRunning)
			elif status=='PENDING' or status=='SUSPENDED' or status=='COMPLETING' or status=='COMPLETED' or status=='COMPLETI':
				currentJobStatus.setStatus(JobStatus.kWaiting)
			elif status=='CANCELLED' or status=='FAILED' or status=='TIMEOUT' or status=='NODE_FAIL':
				currentJobStatus.setStatus(JobStatus.kError)
			else:
				print "Unknown job status", status

			# time
			time_str = lineSplit[5]
			try:
				hours = 0.0
				if '-' in time_str:
					time_str = time_str.split('-')
					hours += float(time_str[0])*24
					time_str = time_str[1].split(':')
				else:
					time_str = time_str.split(':')
				seconds = float(time_str[-1])
				minutes = float(time_str[-2])
				if(len(time_str) > 2):
					hours += float(time_str[-3])
				total_time = hours + minutes / 60.0 + seconds / 3600.0
				currentJobStatus.setCpuTime(total_time, 0)
			except ValueError:
				raise batchelor.BatchelorException("parsing of squeue output to get time information failed. ({0})".format(lineSplit[5]))
		except ValueError:
			raise batchelor.BatchelorException("parsing of squeue output to get job id failed.")

	return jobStates
