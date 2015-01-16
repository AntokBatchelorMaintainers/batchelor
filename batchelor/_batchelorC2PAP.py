
import multiprocessing
import os
import tempfile

import batchelor
from _job import JobStatus



_kMemoryUnits = {'mb': 1.0 / 1024.0, 'gb': 1.0}


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
	return map( lambda j: j.getId(), getListOfJobStates(jobName) )


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
		command += " mgmt." + str(jobId)
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("llcancel failed (stderr: '" + stderr + "')")
	return True



def getListOfJobStates(jobName, username):
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
# 	(fileDescriptor, fileName) = tempfile.mkstemp()
# 	os.close(fileDescriptor)
# 	command = "llq -u `whoami` -m -x &> " + fileName
	command = "llq -u `whoami` -m -x"
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
# 		batchelor.runCommand("rm -f " + fileName)
		raise batchelor.BatchelorException("llq failed (stderr: '" + stderr + "')")
	jobList = []
	jobStates = []
	currentJobId = -1
	currentJobStatus = None;
# 	with open(fileName, 'r') as llqOutput:
	with stdout.split('\n') as llqOutput:
		for line in llqOutput:
			line = line[:-1]
			if line.startswith("===== Job Step mgmt."):
				try:
					currentJobId = int(line[line.find(".")+1:line.rfind(".")])
					currentJobStatus = JobStatus(currentJobId)
				except ValueError:
# 					batchelor.runCommand("rm -f " + fileName)
					raise batchelor.BatchelorException("parsing of llq output to get job id failed.")
			line = ' '.join(line.split())

			if line.startswith("Job Name: "):
				if currentJobId < 0:
# 					batchelor.runCommand("rm -f " + fileName)
					raise batchelor.BatchelorException("parsing of llq output failed, got job name before job id.")
				name = line[10:]
				if name == jobName:
					jobList.append(currentJobId)
					jobStates.append(currentJobStatus)
			elif line.startswith("Step Virtual Memory: "):
				if currentJobId < 0:
# 					batchelor.runCommand("rm -f " + fileName)
					raise batchelor.BatchelorException("parsing of llq output failed, got job name before job id.")
				try:
					parsed = line.lstrip().lstrip('Step Virtual Memory:').split()
					currentJobStatus.setMemoryUsage( float(parsed[0]) * _kMemoryUnits[parsed[1]], 0)
				except ValueError:
# 					batchelor.runCommand("rm -f " + fileName)
					raise batchelor.BatchelorException("parsing of llq output to get job id failed.")
			elif line.startswith("Status: "):
				if currentJobId < 0:
# 					batchelor.runCommand("rm -f " + fileName)
					raise batchelor.BatchelorException("parsing of llq output failed, got job name before job id.")
				else:
					status = line.lstrip().lstrip("Status: ")
					if status == 'Running':
						currentJobStatus.setStatus(JobStatus.kRunning)
					elif status == 'I':
						currentJobStatus.setStatus(JobStatus.kWaiting)
					elif status == 'Submission Error' or status == 'Terminated' or status == 'Removed':
						currentJobStatus.setStatus(JobStatus.kError)
					else:
						currentJobStatus.setStatus(JobStatus.kUnknown)
			elif line.startswith("Step User Time: "):
				if currentJobId < 0:
# 					batchelor.runCommand("rm -f " + fileName)
					raise batchelor.BatchelorException("parsing of llq output failed, got job name before job id.")
				time_str = line.lstrip().lstrip("Step User Time:").split(':')
				try:
					hours = float(time_str[0])
					minuts = float(time_str[1])
					seconds = float(time_str[2])
					total_time = hours + minuts / 60.0 + seconds / 3600.0
					currentJobStatus.setCpuTime(total_time, 0)
				except ValueError:
# 					batchelor.runCommand("rm -f " + fileName)
					raise batchelor.BatchelorException("parsing of llq output to get job id failed.")
# 	batchelor.runCommand("rm -f " + fileName)
	
	return jobStates
	