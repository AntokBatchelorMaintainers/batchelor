
import ConfigParser
import os.path
import subprocess
import time
import signal
import tempfile
import inspect
import pickle
import sys
import re


class BatchelorException(Exception):

	def __init__(self, value):
		self.value = value

	def __str__(self):
		return repr(self.value)
class CancelException(Exception):

	def __init__(self, value):
		self.value = value

	def __str__(self):
		return repr(self.value)


def runCommand(commandString,wd=None):
	commandString = "errHandler() { (( errcount++ )); }; trap errHandler ERR\n" + commandString.rstrip('\n') + "\nexit $errcount"
	kwargs = {}
	if wd is not None:
		kwargs['cwd'] = wd
	process = subprocess.Popen(commandString,
	                           shell=True,
	                           stdout=subprocess.PIPE,
	                           stderr=subprocess.PIPE,
	                           executable="/bin/bash",
	                          **kwargs)
	(stdout, stderr) = process.communicate()
	if stdout:
		stdout = stdout.rstrip(' \n')
	else:
		stdout = ""
	if stderr:
		stderr = stderr.rstrip(' \n')
	else:
		stderr = ""
	return (process.returncode, stdout, stderr)


def detectSystem():
	(returncode, stdout, stderr) = runCommand("hostname -f")
	if returncode != 0:
		raise BatchelorException("runCommand(\"hostname\") failed")
	hostname = stdout
	if hostname.startswith("gridka"):
		raise BatchelorException("hostname '" + hostname + "' seems to indicate gridka, but the wrong host")
	elif hostname == "compass-kit.gridka.de":
		return "gridka"
	elif hostname.startswith("lxplus") or hostname.endswith(".cern.ch"):
		return "lxplusLSF"
	elif hostname.endswith(".e18.physik.tu-muenchen.de"):
		return "e18"
	elif hostname.startswith("ccage"):
		return "lyon"
	elif hostname.startswith("login") and runCommand("which llsubmit")[0] == 0:
		return "c2pap"
	elif hostname.endswith("lrz.de") and runCommand("which sbatch")[0] == 0:
		return "lrz"
	return "UNKNOWN"


def _getRealPath(path):
	return os.path.abspath(os.path.expandvars(os.path.expanduser(path)))


def _checkForSpecialCharacters(string):
	if string is None:
		string = ""
	specialCharacters = [' ', ':', ';', '"', '\'', '@', '!', '?', '$', '\\', '/',
	                     '#', '(', ')', '{', '}', '[', ']', '.', ',', '*']
	foundChars = []
	for char in specialCharacters:
		if string.find(char) > 0:
			foundChars.append(char)
	if foundChars:
		msg = "forbidden characters in job name ("
		for char in foundChars:
			msg += repr(char) + ", "
		msg = msg[:-2]
		msg += ")"
		raise BatchelorException(msg)


def checkConfig(configFileName, system = ""):
	config = ConfigParser.RawConfigParser()
	if not config.read(os.path.abspath(configFileName)):
		print("ERROR: Could not read config file '" + configFileName + "'.")
		return False
	error = False
	if system != "" and not config.has_section(system):
		print("ERROR: System set but corresponding section is missing in config file.")
		error = True
	requiredOptions = { "c2pap": [ "group", "notification", "notify_user", "node_usage", "wall_clock_limit", "resources", "job_type", "class" ],
	                    "e18": [ "memory", "header_file", "arch" ],
	                    "gridka": [ "queue", "project", "memory", "header_file" ],
	                    "lxplus": [ "flavour", "header_file", "memory", "disk" ],
	                    "lxplusLSF": [ "queue", "pool", "header_file" ],
	                    "lyon": [],
	                    "lrz": [ "wall_clock_limit", "memory", "header_file", "max_active_jobs", "clusters", "n_tasks_per_job" ],
	                    "local": [ "shell", "cores" ],
	                    "simulator": [ "lifetime" ] }
	filesToTest = { "gridka": [ "header_file" ],
	                "e18": [ "header_file" ],
	                "lxplus": [ "header_file" ],
	                "lxplusLSF": [ "header_file" ],
	                "c2pap": [ "header_file" ],
	                "lrz": [ "header_file" ],
	                "local": [ "shell" ] }
	for section in requiredOptions.keys():
		if config.has_section(section):
			options = requiredOptions[section]
			for option in options:
				if not config.has_option(section, option):
					print("ERROR: '" + section + "' section is missing option '" + option + "'.")
					error = True
					continue
				if section in filesToTest.keys() and option in filesToTest[section] and (system == "" or system == section):
					path = _getRealPath(config.get(section, option))
					if not os.path.exists(path):
						print("ERROR: Could not find required file '" + path + "'.")
						error = True
	if error:
		return False
	return True


class Batchelor:

	debug = False
	bprintTicker = ""
	batchFunctions = None

	def __init__(self):
		self._config = ConfigParser.RawConfigParser()

	def bprint(self, msg):
		self.bprintTicker += ('' if self.bprintTicker == '' else '\n') + msg
		if self.debug:
			print(msg)

	def initialize(self, configFileName, systemOverride = ""):
		self.bprint("Initializing...")
		if not self._config.read(os.path.abspath(configFileName)):
			self.bprint("Could not read config file '" + configFileName + "'. Initialization failed...")
			return False
		if systemOverride == "":
			self._system = detectSystem()
			if self._system == "UNKNOWN":
				self.bprint("Could not determine on which system we are. Initialization failed...")
				return False
			self.bprint("Detected system '" + self._system + "'.")
		else:
			self._system = systemOverride
			self.bprint("System manually set to '" + self._system + "'.")
		if not self._config.has_section(self._system):
			self.bprint("Could not find section describing '" + self._system +
			            "' in config file '" + configFileName + "'. Initialization failed...")
			return False
		if not checkConfig(configFileName, self._system):
			self.bprint("Config file contains errors. Initialization failed...")
			return False
		self.bprint("Importing appropriate submodule.")
		if self._system == "c2pap":
			import batchelor._batchelorC2PAP as batchFunctions
		elif self._system == "gridka":
			import batchelor._batchelorGridka as batchFunctions
		elif self._system == "e18":
			import batchelor._batchelorE18 as batchFunctions
		elif self._system == "lxplus":
			import batchelor._batchelorLxplusCondor as batchFunctions
		elif self._system == "lxplusLSF":
			import batchelor._batchelorLxplus as batchFunctions
		elif self._system == "lyon":
			import batchelor._batchelorLyon as batchFunctions
		elif self._system == "local":
			import batchelor._batchelorLocal as batchFunctions
			batchFunctions.initialize(self._config)
		elif self._system == "simulator":
			import batchelor._batchelorSimulator as batchFunctions
		elif self._system == "lrz":
			import batchelor._batchelorLRZ as batchFunctions
		else:
			self.bprint("Unknown system '" + self._system + "', cannot load appropriate submodule. Initialization failed...")
			return False
		self.batchFunctions = batchFunctions
		self.bprint("Imported " + batchFunctions.submoduleIdentifier() + " submodule.")
		self.bprint("Initialized.")
		return True

	def initialized(self):
		if self.batchFunctions:
			return True
		else:
			return False

	def shutdown(self):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "shutdown" in self.batchFunctions.__dict__.keys():
			return self.batchFunctions.shutdown()

	def submitJob(self, command, outputFile, jobName = None, wd = None, priority = None, ompNumThreads=None):
		'''
		@param priority: Job priority [-1.0, 1.0]
		@param ompNumThreads: Number of threads requested.
		                      Sets OMP_NUM_THREADS before the command and requires ompNumThreads slots.
		'''
		kwargs = {}
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "submitJob" in self.batchFunctions.__dict__.keys():
			_checkForSpecialCharacters(jobName)
			if priority is not None:
				priority = float(priority)
				if priority < -1.0 or priority > 1.0:
					raise BaseException("Priority must be within [-1.0, 1.0]")
				if not 'priority' in inspect.getargspec(self.batchFunctions.submitJob)[0]:
					raise BatchelorException("Priority not implemented")
				kwargs['priority'] = priority
			if ompNumThreads is not None:
				ompNumThreads = int(ompNumThreads)
				if not 'ompNumThreads' in inspect.getargspec(self.batchFunctions.submitJob)[0]:
					raise BatchelorException("ompNumThreads not implemented")
				kwargs['ompNumThreads'] = ompNumThreads

			return self.batchFunctions.submitJob(self._config, command, outputFile, jobName, wd, **kwargs)
		else:
			raise BatchelorException("not implemented")

	def submitJobs(self, jobs):
		# 'jobs' should be a list of arguments as they need to be specified for
		# 'submitJob', e.g.:
		#     [ [ "command 1", "output file 1", "name 1" ],
		#       [ "command 2", "output file 2", None ],
		#       ... ]
		# The return value is a list of job IDs in the same order as the jobs.
		# A job ID of -1 indicates an error during submission of this job.
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "submitJobs" in self.batchFunctions.__dict__.keys():
			for i in range(len(jobs)):
				if len(jobs[i]) == 3:
					_checkForSpecialCharacters(jobs[i][2])
				elif len(jobs[i]) == 2:
					# the 'submitJob' method of the 'Batchelor' class
					# has a default argument for the job name, do
					# something similar here
					jobs[i].append(None)
				else:
					raise BatchelorException("wrong number of arguments")
			return self.batchFunctions.submitJobs(self._config, jobs)
		else:
			jobIds = []
			for job in jobs:
				try:
					jobId = self.submitJob(*job)
				except BatchelorException as exc:
					jobId = -1
				jobIds.append(jobId)
			return jobIds

	def submitArrayJobs(self, commands, outputFile, jobName = None, wd = None):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "submitArrayJobs" in self.batchFunctions.__dict__.keys():
			_checkForSpecialCharacters(jobName)

			return self.batchFunctions.submitArrayJobs(self._config, commands, outputFile, jobName, wd)
		else:
			raise BatchelorException("not implemented")

	def getListOfActiveJobs(self, jobName = None):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "getListOfActiveJobs" in self.batchFunctions.__dict__.keys():
			_checkForSpecialCharacters(jobName)
			return self.batchFunctions.getListOfActiveJobs(jobName)
		else:
			raise BatchelorException("not implemented")

	def getNActiveJobs(self, jobName = None):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "getNActiveJobs" in self.batchFunctions.__dict__.keys():
			_checkForSpecialCharacters(jobName)
			return self.batchFunctions.getNActiveJobs(jobName)
		else:
			raise BatchelorException("not implemented")

	def jobStillRunning(self, jobId):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "jobStillRunning" in self.batchFunctions.__dict__.keys():
			return self.batchFunctions.jobStillRunning(jobId)
		else:
			raise BatchelorException("not implemented")

	def getListOfErrorJobs(self, jobName = None):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "getListOfErrorJobs" in self.batchFunctions.__dict__.keys():
			_checkForSpecialCharacters(jobName)
			return self.batchFunctions.getListOfErrorJobs(jobName)
		else:
			raise BatchelorException("not implemented")

	def getListOfWaitingJobs(self, jobName = None):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "getListOfWaitingJobs" in self.batchFunctions.__dict__.keys():
			_checkForSpecialCharacters(jobName)
			return self.batchFunctions.getListOfWaitingJobs(jobName)
		else:
			raise BatchelorException("not implemented")

	def getListOfRunningJobs(self, jobName = None):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "getListOfRunningJobs" in self.batchFunctions.__dict__.keys():
			_checkForSpecialCharacters(jobName)
			return self.batchFunctions.getListOfRunningJobs(jobName)
		else:
			raise BatchelorException("not implemented")

	def resetErrorJobs(self, jobName = None):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "resetErrorJobs" in self.batchFunctions.__dict__.keys():
			_checkForSpecialCharacters(jobName)
			return self.batchFunctions.resetErrorJobs(jobName)
		else:
			raise BatchelorException("not implemented")

	def deleteErrorJobs(self, jobName = None):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "deleteErrorJobs" in self.batchFunctions.__dict__.keys():
			_checkForSpecialCharacters(jobName)
			return self.batchFunctions.deleteErrorJobs(jobName)
		else:
			raise BatchelorException("not implemented")

	def deleteJobs(self, jobIds):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "deleteJobs" in self.batchFunctions.__dict__.keys():
			return self.batchFunctions.deleteJobs(jobIds)
		else:
			raise BatchelorException("not implemented")

	def getListOfJobStates(self, jobIDs = None, username = None):
		'''
		Get the job stats of all jobs
		@param jobIDs: Lock only of jobs with the given jobIDs
		@param username: Look only of jobs of the given username
		@return: List of all job states of all active jobs
		@rtype: list
		'''
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "getListOfJobStates" in self.batchFunctions.__dict__.keys():
			return self.batchFunctions.getListOfJobStates(jobIDs, username)
		else:
			raise BatchelorException("not implemented")



class BatchelorHandler(Batchelor):
	'''
		Specialization of the Batchelor class
		to also handle running jobs
	'''

	def __init__(self, configfile = '~/.batchelorrc', systemOverride = "", n_threads = -1, memory = None, check_job_success = False, store_commands = False, catchSIGINT= True, collectJobs = False):
		'''
		Initialize the batchelor
		@param configfile: Path to batchelor configfile
		@param systemOverride: Manual selection of the execution system ('local', 'E18', ...).
		@param n_threads: Number of threads for local processing.
		@param memory: Set used memory per job (e.g. 500M).
		@param check_job_success: Check if the job has been finished successfully.
		@param store_commands: Store commands in a dedicated pickle file to reschedule commands. (Also a folder name can be given)
		@param catchSIGINT: Catch SIGINT (Ctrl+C) and ask to stopp all jobs
		@param collectJobs: Collect jobs. You can submit them later by using `submitCollectedJobsInArray`. This enables the internal job-id counter.
		'''


		Batchelor.__init__(self)

		Batchelor.initialize(self, os.path.expanduser(configfile), systemOverride )

		if systemOverride == "local" and n_threads >= 0:
			self._config.set("local","cores", n_threads);

		if memory:
			self.setMemory(memory)

		self._submittedJobs = [] # list of job ids of the batch system that have actually been submitted to the batch system
		self._jobIds   = []      # list of job ids submitted throuth this BatchelorHandler. The same as _submittedJobs if not _collectJobs
		self._commands = []
		self._logfiles = []
		self._check_job_success = check_job_success
		self._store_commands = store_commands
		self._store_commands_filename = ""
		self._collectJobs = collectJobs
		self._collectedJobs = []
		self._internalJidCounter=0

		if self._store_commands:
			self._store_commands_filename = os.path.join(time.strftime("batchelorComandsLog_%y-%m-%d_%H-%M-%S.dat"))
			if isinstance(self._store_commands, str) and os.path.isdir(self._store_commands):
				self._store_commands_filename = os.path.join(self._store_commands, self._store_commands_filename)
			else:
				self._store_commands_filename = os.path.join(os.getcwd(), self._store_commands_filename)

		def finish(signal, frame):
			print
			if raw_input("You pressed Ctrl+C. Cancel all jobs? [y/N]:") == 'y':
				print "stopping all jobs and shutting down batchelor..."
				self.deleteJobs( self.getListOfSubmittedActiveJobs())
				time.sleep(3);
				self.shutdown()
				print "Done"
				raise CancelException( "Catched Ctrl+C" );
			else:
				print "continuing.."

		if catchSIGINT:
			signal.signal( signal.SIGINT, finish)


	def setMemory(self, memory):
		'''
		Set memory requested per job. Overwrites settings from config file
		'''
		for section in self._config.sections():
			self._config.set(section, "memory", memory)


	def submitJob(self, command, output = '/dev/null', wd = None, jobName=None, priority = None, ompNumThreads = None):
		'''
		Submit job with the given command

		@param command: Command to be executed
		@param wd: Working directory. Default = current workingdirectory
		@param output: Path or directory of log files. If not given, but check_job_success is selected, a .log folder will be created in the wd
		@param jobName: Name of the submitted job. Default='Batchelor'

		@return: jobID
		'''
		if not jobName:
			jobName = 'Batchelor'
		if not wd:
			wd = os.getcwd()

		if output == '/dev/null' and self._check_job_success: # if not output file is given but the job success should be checked anyway
			logdir = os.path.join(wd, '.log')
			if not os.path.isdir(logdir):
				os.makedirs( logdir )
			output = tempfile.mktemp(prefix = time.strftime("%Y-%m-%d_%H-%M-%S_"),suffix = '.log', dir = logdir)

		if self._check_job_success:
			command = command + " && echo \"BatchelorStatus: OK\" || (s=$?; echo \"BatchelorStatus: ERROR ($s)\"; exit $s)"


		if not self._collectJobs:
			jid = Batchelor.submitJob(self, command, outputFile = output, jobName=jobName, wd=wd, priority = priority, ompNumThreads=ompNumThreads)
		else:
			self._collectedJobs.append(len(self._commands))
			jid = -1

		if jid:
			self._submittedJobs.append(jid)
			if self._collectJobs:
				self._jobIds.append(self._internalJidCounter)
				jid = self._internalJidCounter
				self._internalJidCounter += 1
			else:
				self._jobIds.append(jid)
			self._commands.append( command )
			self._logfiles.append( output )

			if self._store_commands:
				with open(self._store_commands_filename, 'a') as fout:
					submit_entry = {'command': command, 'output': output, 'jobName': jobName, 'wd': wd, 'priority': priority, 'ompNumThreads': ompNumThreads}
					submit = {self._jobIds[-1]:submit_entry}
					pickle.dump(submit, fout, protocol=2)

		return jid


	def getListOfSubmittedActiveJobs(self, jobName=None):
		'''
		Get list of all running jobs, which have been submitted by this instance
		@return: List of running jobs
		'''

		return [ j for j in self.getListOfActiveJobs(jobName) if j in self._submittedJobs ]


	def collectJobsIfPossible(self, verbose=False):
		'''
		Collect jobs if implemented for the current batch system
		@param verbose: Print warning if collections is not implemented
		@return: True if job collections is active
		'''
		if "canCollectJobs" in self.batchFunctions.__dict__.keys() and self.batchFunctions.canCollectJobs():
			self._collectJobs = True
		elif verbose:
			print "Collection of jobs is not implemented for the current batch system."

		if self._submittedJobs:
			print "Using `collectJobs`, but {0} jobs have been already submitted.".format(len(self._submittedJobs))

		return self._collectJobs


	def submitCollectedJobsInArray(self, outputFile = "/dev/null", jobName=None, wd = None):
		if not self._collectedJobs:
			return []
		if not wd:
			wd = os.getcwd()
		commands = ["( {0} ) &> '{1}'".format(self._commands[i], self._logfiles[i]) for i in self._collectedJobs]
		self._collectedJobs = []
		if outputFile == "/dev/null" and self._logfiles[0] != "/dev/null":
			outputFile = os.path.join(os.path.dirname(self._logfiles[0]), 'master.log')
		self._submittedJobs = Batchelor.submitArrayJobs(self, commands, outputFile = outputFile, wd=wd, jobName=jobName)
		return self._submittedJobs


	def wait(self, timeout = 60, jobName = None):
		'''
		Wait for all jobs, submitted by this instance, to be finished

		@param timeout: Timeout in seconds between checking the joblist
		@param jobName: Only wait for jobs with the given job-name
		'''


		while True:
			try:
				running_jobs = self.getListOfSubmittedActiveJobs(jobName)
			except BatchelorException as e:
				print "Error when fetching running jobs"
				print e
				running_jobs = [-1] # dummy job to stay in the loop
			if not running_jobs:
				break
			if self.debug:
				print "Waiting for jobs:", running_jobs;
			time.sleep(timeout)


		return;


	def checkJobStates(self, verbose=True, raiseException=False):
		'''
		Check the status of the jobs submitted by this `BatchelorHandler`
		@param verbose: Print information about failed jobs
		@param raiseException: Raise exception if one or more jobes failed
		'''
		if not self._check_job_success:
			print "Called checkJobStates, but Batchelor was not configured to check job states"
			return False

		error_ids = []
		error_logfiles = []
		for i_job, log_file in enumerate( self._logfiles ):
			found = False
			for _ in xrange(10): # 10 trails to wait for log file
				if os.path.isfile(log_file):
					found = True
					break
				time.sleep(6)
			if not found:
				if verbose:
					if not error_ids: # first found error
						print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
					print "Can not find logfile '{0}'".format(log_file)
					print "\tfor command:'{0}'".format(self._commands[i_job])
				error_ids.append( self._jobIds[i_job])
				error_logfiles.append(log_file)
			else:
				if not self._checkJobStatus(log_file):
					if verbose:
						if not error_ids: # first found error
							print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
						print "Error in logfile '{0}'".format(log_file)
						print "\tfor command:'{0}'".format(self._commands[i_job])
					error_ids.append( self._jobIds[i_job])
					error_logfiles.append(log_file)

		if raiseException and len(error_ids) > 0:
			raise BatchelorException("{0} jobs failed".format(len(error_ids)))
		return error_ids, error_logfiles


	def _checkJobStatus(self, log_file):
		foundOK = False
		foundERROR = False
		with open(log_file) as fin:
			for line in fin:
				if re.match(r"^BatchelorStatus: OK$", line):
					foundOK = True
				if re.match(r"^BatchelorStatus: ERROR", line):
					foundERROR = True
		return not (foundERROR or not foundOK)


	def resubmitStoredJobs(self, jobs_filename, jobids_to_submit = None):
		'''
		@param jobids_to_submitparam: If not given, all jobs are resubmitted
		@return ids of resubmitted jobs {old_jobid: new_jobid}
		'''
		jobs = []
		with open(jobs_filename) as fin:
			while True:
				try:
					j = pickle.load(fin)
					jobs.append(j)
				except EOFError:
					break;
		# change to dictionary
		jobs = { j.keys()[0]: j.values()[0] for j in jobs }

		if not jobids_to_submit:
			jobids_to_submit = sorted(jobs.keys());

		key_type = type(jobs.keys()[0])
		new_jobids = {}
		for jid in jobids_to_submit:
			jid = key_type(jid)
			if not jid in jobs:
				raise BatchelorException("Can not resubmit jobid '{0}'. Not in given jobs file.".format(jid))
			# append old output file to outputfile.old
			outputFile = jobs[jid]['output']
			if os.path.isfile(outputFile):
				with open(outputFile+".old", "a") as fout:
					with open(outputFile) as fin:
						fout.write(fin.read())
				os.remove(outputFile)
			print "Resubmit job:", jid
			for k in sorted(jobs[jid].keys()):
				print "\t{0}: {1}".format(k, jobs[jid][k])
			new_jid = self.submitJob( **jobs[jid] )
			new_jobids[jid] = new_jid

		return new_jobids
