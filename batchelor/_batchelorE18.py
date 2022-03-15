
import ConfigParser
import glob
import os
import tempfile
import xml.etree.ElementTree as ElementTree
import xml.etree.ElementTree
import re

import batchelor
from _job import JobStatus


def submoduleIdentifier():
	return "e18"

def canCollectJobs():
	return True

def submitJob(config, command, outputFile, jobName, wd = None, arrayStart = None, arrayEnd = None, arrayStep = None, priority=None, ompNumThreads=None):

	# some checks of the job-settings
	if wd and os.path.realpath(wd).count(os.path.realpath(os.path.expanduser('~'))):
		raise batchelor.BatchelorException("The given working-directory is in your home-folder which is no allowed at E18: '{0}'".format(wd))

	if os.path.realpath(outputFile).count(os.path.realpath(os.path.expanduser('~'))):
		raise batchelor.BatchelorException("The given output-file is in your home-folder which is no allowed at E18: '{0}'".format(outputFile))

	if priority:
		priority = max(int(-1024 + 2048 * (priority+1.0)/2.0), -1023)

	(fileDescriptor, fileName) = tempfile.mkstemp()
	os.close(fileDescriptor)
	batchelor.runCommand("cp " + batchelor._getRealPath(config.get(submoduleIdentifier(), "header_file")) + " " + fileName)
	with open(fileName, 'a') as scriptFile:
		if ompNumThreads is not None:
			scriptFile.write("export OMP_NUM_THREADS={0}\n".format(ompNumThreads))
		scriptFile.write(command)
	cmnd = "qsub "
	cmnd += "-j y "
	cmnd += "-b no "
	cmnd += "-m n "
	cmnd += "" if jobName is None else ("-N " + jobName + " ")
	if arrayStart is not None:
		cmnd += "-t " + str(arrayStart) + "-" + str(arrayEnd) + ":" + str(arrayStep) + " "
	cmnd += "-o '" + outputFile + "' "
	cmnd += "-wd '" + ("/tmp/" if not wd else wd) + "' "
	if config.has_option(submoduleIdentifier(), "shortqueue") and config.get(submoduleIdentifier(), "shortqueue") in [1, "1", "TRUE", "true", "True"]:
		cmnd += "-l short=1 "
	elif config.has_option(submoduleIdentifier(), "longqueue") and config.get(submoduleIdentifier(), "longqueue") in [1, "1", "TRUE", "true", "True"]:
		cmnd += "-l long=1 "
	elif config.has_option(submoduleIdentifier(), "ioqueue") and config.get(submoduleIdentifier(), "ioqueue") in [1, "1", "TRUE", "true", "True"]:
		cmnd += "-l io=1 "
	else:
		cmnd += "-l medium=1 "
	cmnd += "-l h_pmem=" + config.get(submoduleIdentifier(), "memory") + " "
	cmnd += "-l arch=" + config.get(submoduleIdentifier(), "arch") + " "
	cmnd += _getExcludedHostsString(config)
	cmnd += "-p {0} ".format(priority) if priority else ""
	cmnd += "-pe mt {0} ".format(ompNumThreads) if ompNumThreads is not None else ""
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


def submitArrayJobs(config, commands, outputFile, jobName, wd = None):
	outputFileOrig = outputFile
	nTasksPerJob=int(config.get(submoduleIdentifier(), "n_tasks_per_job"))
	i = 0
	jids = []
	while i < len(commands):
		j = min(len(commands), i+nTasksPerJob)
		nTasks = j-i
		fullCmd = ""
		for k, ii in enumerate(range(i,j)):
			fullCmd += 'if [[ ${{SGE_TASK_ID}} == {i} ]]; then {cmd}; fi\n'.format(cmd=commands[ii], i=k+1)
		if outputFile != "/dev/null":
			outputFile = outputFileOrig + (".{0}_{1}".format(i,j) if len(commands) > nTasksPerJob else "")
		jid = submitJob(config, fullCmd, outputFile, jobName, wd, arrayStart=1, arrayEnd=nTasks, arrayStep=1)
		jids += [jid]*nTasks
		i=j
	return jids


def getListOfActiveJobs(jobName):
	if jobName is None:
		command = "qstat"
		(returncode, stdout, stderr) = batchelor.runCommand(command)
		if returncode != 0:
			raise batchelor.BatchelorException("qstat failed (stderr: '" + stderr + "')")
		if stdout == "":
			return []
		jobList = stdout.split('\n')[2:]
		try:
			return [ int(job.split()[0]) for job in jobList ]
		except ValueError:
			raise batchelor.BatchelorException("parsing of qstat output to get job id failed.")
	command = "qstat -j " + jobName
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		if stderr.split('\n')[0][:-1] == "Following jobs do not exist:":
			return []
		raise batchelor.BatchelorException("qstat failed (stderr: '" + stderr + "')")
	(fileDescriptor, fileName) = tempfile.mkstemp()
	os.close(fileDescriptor)
	command = "qstat -xml -j " + jobName + " > " + fileName
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("qstat failed (stderr: '" + stderr + "')")
	batchelor.runCommand("awk '/<\?xml version='\"'\"'1.0'\"'\"'\?>/{n++}{print >\"" + fileName + "\" n \".awkOut\" }' " + fileName)
	batchelor.runCommand("rm -f " + fileName)
	xmlFiles = glob.glob(fileName + "*.awkOut")
	jobIds = []
	for xmlFile in xmlFiles:
		tree = ElementTree.parse(xmlFile)
		root = tree.getroot()
		batchelor.runCommand("rm -f " + xmlFile)
		for child in root[0]:
			jobIdList = child.findall("JB_job_number")
			if len(jobIdList) != 1:
				raise batchelor.BatchelorException("parsing xml from qstat failed")
			try:
				jobId = int(jobIdList[0].text)
			except ValueError:
				raise batchelor.BatchelorException("parsing int from xml from qstat failed")
			jobIds.append(jobId)
	return jobIds


def getNActiveJobs(jobName):
	return len(getListOfActiveJobs(jobName))


def jobStillRunning(jobId):
	if jobId in getListOfActiveJobs(str(jobId)):
		return True
	else:
		return False


def getListOfErrorJobs(jobName):
	listOfActiveJobs = getListOfActiveJobs(jobName)
	command = "qstat"
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("qstat failed (stderr: '" + stderr + "')")
	qstatLines = stdout.split('\n')[2:]
	listOfErrorJobs = []
	for line in qstatLines:
		lineList = line.split()
		jobId = -1
		try:
			jobId = int(lineList[0])
		except ValueError:
			raise batchelor.BatchelorException("parsing of qstat output to get job id failed.")
		if jobId not in listOfActiveJobs:
			continue
		if lineList[4] == "Eqw":
			listOfErrorJobs.append(jobId)
	return listOfErrorJobs

def getListOfWaitingJobs(jobName):
	listOfActiveJobs = getListOfActiveJobs(jobName)
	command = "qstat"
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("qstat failed (stderr: '" + stderr + "')")
	qstatLines = stdout.split('\n')[2:]
	listOfErrorJobs = []
	for line in qstatLines:
		lineList = line.split()
		jobId = -1
		try:
			jobId = int(lineList[0])
		except ValueError:
			raise batchelor.BatchelorException("parsing of qstat output to get job id failed.")
		if jobId not in listOfActiveJobs:
			continue
		if lineList[4] == "qw":
			listOfErrorJobs.append(jobId)
	return listOfErrorJobs

def getListOfRunningJobs(jobName):
	listOfActiveJobs = getListOfActiveJobs(jobName)
	command = "qstat"
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("qstat failed (stderr: '" + stderr + "')")
	qstatLines = stdout.split('\n')[2:]
	listOfErrorJobs = []
	for line in qstatLines:
		lineList = line.split()
		jobId = -1
		try:
			jobId = int(lineList[0])
		except ValueError:
			raise batchelor.BatchelorException("parsing of qstat output to get job id failed.")
		if jobId not in listOfActiveJobs:
			continue
		if lineList[4] == "r":
			listOfErrorJobs.append(jobId)
	return listOfErrorJobs

def resetErrorJobs(jobName):
	for id in getListOfErrorJobs(jobName):
		command = "qmod -cj " + str(id)
		(returncode, stdout, stderr) = batchelor.runCommand(command)
		if stdout.find('cleared error state of job') is -1:
			raise batchelor.BatchelorException("qmod failed (stderr: '" + stderr + "')")
	return True



def deleteErrorJobs(jobName):
	return deleteJobs(getListOfErrorJobs(jobName))


def deleteJobs(jobIds):
	if not jobIds:
		return True
	command = "qdel"
	for jobId in jobIds:
		command += " " + str(jobId)
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("qdel failed (stderr: '" + stderr + "')")
	return True




def getListOfJobStates(select_jobIDs, username):


	# get list of all jobs
	if username == None:
		command = "qstat"
	else:
		command = "qstat -u {0}".format(username)

	(returncode, stdout, stderr) = batchelor.runCommand(command)

	if returncode != 0:
		raise batchelor.BatchelorException("qstat failed (stderr: '" + stderr + "')")

	if stdout == "":
		return []

	jobList = stdout.split('\n')[2:]

	try:
		jobIDs = [ int(job.split()[0]) for job in jobList ]
		jobStates = [ job.split()[4] for job in jobList ];
	except ValueError:
		raise batchelor.BatchelorException("parsing of qstat output to get job id failed.")

	list_of_states = [];

	for i, jobID in enumerate(jobIDs):
		if select_jobIDs == None or jobID in select_jobIDs:
			job_status = JobStatus(jobID);
			job_status.setStatus( JobStatus.kUnknown, name = jobStates[i] );

			if jobStates[i] == 'qw' or jobStates[i] == 'hqw':
				job_status.setStatus( JobStatus.kWaiting );

			elif jobStates[i] == 't':
				job_status.setStatus( JobStatus.kTransmitting )

			elif jobStates[i] == 'd' or jobStates[i] == 'dr' or jobStates[i] == 'dt':
				job_status.setStatus( JobStatus.kDeletion)

			elif jobStates[i] == 'Eq':
				job_status.setStatus( JobStatus.kError );

			elif jobStates[i] == 'r' or jobStates[i] == 'hr':

				# get detailed job information
				command = "qstat -xml -j {0}".format(jobID);
				(returncode, stdout, stderr) = batchelor.runCommand(command)
				if returncode != 0:
					raise batchelor.BatchelorException("qstat failed (stderr: '" + stderr + "')")
				elif 'unknown_jobs' in stdout:
					continue; # the job has been ended between the qstat command and now
				else:
					try:
						root = ElementTree.fromstring( stdout );
						for child in root[0]:
							for task in child.findall('JB_ja_tasks'):
								for sublist in task.findall('ulong_sublist'):
									task_number = sublist.findall('JAT_task_number')
									if task_number:
										task_number = int(task_number[0].text)
										job_status.setStatus( JobStatus.kRunning );
										for usage_list in sublist.findall('JAT_scaled_usage_list'):
											for scaled in usage_list.findall('scaled'):
												name = scaled.findall('UA_name')[0].text
												value = scaled.findall('UA_value')[0].text
												if name == 'cpu':
													job_status.setCpuTime(float(value) / 3600.0, task_number);
												elif name == 'vmem':
													job_status.setMemoryUsage(float(value) / (1024.0)**3, task_number);
					except xml.etree.ElementTree.ParseError as e:
						raise batchelor.BatchelorException("xml-parser could not parse output of qstat -xml -j {0}: {1}".format(jobID, e))

					# end of parsing through the xml tree



			list_of_states.append( job_status );


		# end of if jobs belongs to the selected jobs
	# end of loop over all jobs

	return list_of_states;









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
