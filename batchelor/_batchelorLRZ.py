
import multiprocessing
import os
import sys
import tempfile
import time

import batchelor
from _job import JobStatus




def submoduleIdentifier():
	return "lrz"

def canCollectJobs():
	return True


def _submitJob(config, command, outputFile, jobName, wd = None, nTasks=None):



	# check if only a certain amount of active jobs is allowd
	if config.has_option(submoduleIdentifier(), "max_active_jobs"):
		max_active_jobs = int(config.get(submoduleIdentifier(), "max_active_jobs"))
		i=0;
		waitTime = 90
		while True:
			try:
				nRunningJobs = len(getListOfActiveJobs(None))
			except batchelor.BatchelorException:
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
		if config.get(submoduleIdentifier(), "clusters") != 'mpp3' and config.get(submoduleIdentifier(), "clusters") != 'c2pap':
			tempFile.write("#SBATCH --mem-per-cpu=" + config.get(submoduleIdentifier(), "memory") + "\n")
		if jobName is not None:
			tempFile.write("#SBATCH -J " + jobName + "\n")
		tempFile.write("#SBATCH --get-user-env \n")
		tempFile.write("#SBATCH --export=NONE \n")
		if nTasks is not None:
			if config.get(submoduleIdentifier(), "clusters") != 'mpp3':
				tempFile.write("#SBATCH --ntasks={0:d} \n".format(nTasks))
			else:
				tempFile.write("#SBATCH --nodes={0:d} \n".format((nTasks+63)//64))
			tempFile.write("#SBATCH --ntasks-per-node={0} \n".format(config.get(submoduleIdentifier(), "n_tasks_per_node")))
		tempFile.write("#SBATCH --clusters={0}\n".format(config.get(submoduleIdentifier(), "clusters")))
		if config.get(submoduleIdentifier(), "clusters") not in [ 'cm2_tiny', 'mpp3']:
			tempFile.write("#SBATCH --partition={0}\n\n".format(config.get(submoduleIdentifier(), "partition")))
		if config.get(submoduleIdentifier(), "clusters") == 'cm2' or config.get(submoduleIdentifier(), "clusters") == 'c2pap':
			tempFile.write("#SBATCH --qos={0}\n\n".format(config.get(submoduleIdentifier(), "partition")))
		tempFile.write("module load slurm_setup \n\n\n")
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



def submitJob(config, command, outputFile, jobName, wd = None):
	return _submitJob(config, command, outputFile, jobName, wd)

def submitArrayJobs(config, commands, outputFile, jobName, wd = None):
	nTasksPerJob=int(config.get(submoduleIdentifier(), "n_tasks_per_job"))
	i = 0
	jids = []
	outputFileOrig = outputFile
	headerFileName = batchelor._getRealPath(config.get(submoduleIdentifier(), "header_file"))
	with open(headerFileName, 'r') as headerFile:
		header = headerFile.read().replace(r'"', r'\"')
	while i < len(commands):
		j = min(len(commands), i+nTasksPerJob)
		nTasks = j-i
		srunConf = "\n".join(["{i} {cmd}".format(i=ii, cmd=commands[ii]) for ii in range(i,j)])
		srunConf = srunConf.replace(r'"', r'\"')
		tmpDir = os.path.join(os.environ['SCRATCH'], 'tmp')
		if not os.path.isdir(tmpDir):
			os.makedirs(tmpDir)
		fullCmd = 'tmpDir=$(mktemp -d -p {TMPDIR})\ntrap "rm -rf \'${{tmpDir}}\'" EXIT\n'.format(TMPDIR=tmpDir)
		fullCmd += 'echo "{srun}" > ${{tmpDir}}/srun.conf\n'.format(srun='\n'.join(["{i} bash ${{tmpDir}}/{i}.sh".format(i=k) for k in range(nTasks)]))
		for k, ii in enumerate(range(i,j)):
			fullCmd += 'echo "#!/bin/bash\n{header}\n{cmd}" > ${{tmpDir}}/{i}.sh\n'.format(header=header, cmd=commands[ii].replace(r'"', r'\"'), i=k)
		fullCmd += 'srun -n {nTasks} --multi-prog ${{tmpDir}}/srun.conf'.format( nTasks=nTasks)
		if outputFile != "/dev/null" and len(commands) > nTasksPerJob:
			outputFile = outputFileOrig + ".{0}_{1}".format(i,j)
		jid = _submitJob(config, fullCmd, outputFile, jobName, wd, nTasks= nTasks)
		jids += [jid]*nTasks
		i=j
	return jids


def _wrapSubmitJob(args):
	try:
		return submitJob(*args)
	except batchelor.BatchelorException as exc:
		return -1


def getListOfActiveJobs(jobName):
	ret = map( lambda j: j.getId(), getListOfJobStates(jobName, detailed=False) )
	return ret


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
	command = "scancel --clusters=all"
	for jobId in jobIds:
		command += " " + str(jobId)
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("scancel failed (stderr: '" + stderr + "')")
	return True



def getListOfJobStates(jobName, username = None, detailed = True):
	command = "squeue --clusters=all -u $(whoami) -l -h"
	(returncode, stdout, stderr) = batchelor.runCommand(command)
	if returncode != 0:
		raise batchelor.BatchelorException("squeue failed (stderr: '" + stderr + "')")
	jobList = []
	jobStates = []
	for line in stdout.split('\n'):
		if line.startswith("CLUSTER: "):
			continue;
		if line.strip().startswith("JOBID"):
			continue;
		line = line.rstrip('\n')
		if not line:
			continue
		lineSplit = line.split()
		try:
			if '_' in lineSplit[0]:
				currentJobId = int(lineSplit[1].split('_')[0])
			else:
				currentJobId = int(lineSplit[1])
			currentJobStatus = JobStatus(currentJobId)

			# name
			name = lineSplit[3]
			if name == jobName or jobName == None:
				jobList.append(currentJobId)
				jobStates.append(currentJobStatus)

			# status
			status = lineSplit[5]
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
			time_str = lineSplit[6]
			try:
				hours = 0.0
				minutes = 0.0
				seconds = 0.0
				if time_str == 'INVALID':
					pass
				else:
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
				raise batchelor.BatchelorException("parsing of squeue output to get time information failed. ({0})".format(lineSplit[6]))
		except ValueError:
			raise batchelor.BatchelorException("parsing of squeue output to get job id failed.")

	return jobStates
