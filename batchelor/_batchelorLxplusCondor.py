
import ConfigParser
import os
import tempfile
import random
import string
import atexit

import batchelor


def submoduleIdentifier():
	return "lxplus"


def submitJob(config, command, outputFile, jobName, wd = None, arrayStart = None, arrayEnd = None, arrayStep = None):
	if arrayStart is not None or arrayEnd is not None or arrayStep is not None:
		raise BatchelorException("Array jobs are not (yet) implementet for CERNs HTCondor system")

	filesDir = os.path.join(os.getcwd(), '.log')
	if " " in filesDir:
		raise BatchelorException("Cannot handle submit directories with whitespaces")

	if not os.path.exists(filesDir):
		os.makedirs(filesDir)
	(fileDescriptor, submitFileName) = tempfile.mkstemp(dir=filesDir, prefix='submitFiles_', suffix='.submit')
	os.close(fileDescriptor)
	atexit.register(lambda: os.remove( submitFileName ))
	(fileDescriptor, scriptFileName) = tempfile.mkstemp(dir=filesDir, prefix='scriptFiles_', suffix='.sh')
	os.close(fileDescriptor)
	atexit.register(lambda: os.remove( scriptFileName ))
	os.chmod(scriptFileName, 0755)

	batchelor.runCommand("cp " + batchelor._getRealPath(config.get(submoduleIdentifier(), "header_file")) + " " + scriptFileName)
	with open(scriptFileName, 'a') as scriptFile:
		scriptFile.write(command)
	with open(submitFileName, 'w') as submitFile:
		outputFile = os.path.abspath(outputFile)
		submitFile.write("executable = {0}\n".format(scriptFileName))
		if outputFile:
			submitFile.write("output = {0}\n".format(outputFile))
			submitFile.write("log = {0}.condor\n".format(outputFile))
			submitFile.write("error = {0}.err\n".format(outputFile))
		submitFile.write("request_cpus  = 1\n")
		submitFile.write("request_memory = {0}\n".format(config.get(submoduleIdentifier(), "memory")))
		submitFile.write("request_disk = {0}\n".format(config.get(submoduleIdentifier(), "disk")))
		submitFile.write("+JobFlavour = \"{0}\"\n".format(config.get(submoduleIdentifier(), "flavour")))
		submitFile.write("queue 1\n")
	cmnd = "condor_submit '{0}'".format(submitFileName)
	if jobName:
		cmnd += " -batch-name {0} ".format(jobName)
	kwargs = {}
	if wd:
		kwargs['wd'] = wd
	(returncode, stdout, stderr) = batchelor.runCommand(cmnd, **kwargs)
	if returncode != 0:
		raise batchelor.BatchelorException("condor_submit failed (stderr: '" + stderr + "')")
	jobId = stdout.split('\n')[1].split()[5].rstrip(".")
	try:
		jobId = int(jobId)
	except ValueError:
		raise batchelor.BatchelorException('parsing of condor_submit output to get job id failed.')
	return jobId


def getListOfActiveJobs(jobName):
	command = "condor_q   -format \"%d.\" ClusterId -format \"%d\n\" ProcId "
	if jobName:
		command += "-constraint 'JobBatchName == \"{0}\"' ".format(jobName)
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("condor_q failed (stderr: '" + stderr + "')")
	if stdout == "":
		return []
	jobList = stdout.split('\n')
	jobs = []
	for job in jobList:
		job = job.split()
		if len(job) > 0:
			try:
				jobID = int(job[0].rstrip(".0"))
				jobs.append(jobID)
			except ValueError:
				raise batchelor.BatchelorException("Cannot parse return of condor_q (stdout: '" + stdout + "')")
	return jobs


def getNActiveJobs(jobName):
	return len(getListOfActiveJobs(jobName))


def jobStillRunning(jobId):
	if jobId in getListOfActiveJobs(str(jobId)):
		return True
	else:
		return False


def deleteJobs(jobIds):
	if not jobIds:
		return True
	command = "condor_rm"
	for jobId in jobIds:
		command += ' ' + str(jobId)
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		if not 'Couldn\'t find/remove all jobs matching constraint' in stderr:
			raise batchelor.BatchelorException("condor_rm failed (stderr: '" + stderr + "')")
	return True

