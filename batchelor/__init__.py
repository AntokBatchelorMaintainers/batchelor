
import ConfigParser
import os.path
import subprocess
import inspect


class BatchelorException(Exception):

	def __init__(self, value):
		self.value = value

	def __str__(self):
		return repr(self.value)


def runCommand(commandString):
	commandString = "errHandler() { (( errcount++ )); }; trap errHandler ERR\n" + commandString.rstrip('\n') + "\nexit $errcount"
	process = subprocess.Popen(commandString,
	                           shell=True,
	                           stdout=subprocess.PIPE,
	                           stderr=subprocess.PIPE,
	                           executable="/bin/bash")
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
	(returncode, stdout, stderr) = runCommand("hostname")
	if returncode != 0:
		raise BatchelorException("runCommand(\"hostname\") failed")
	hostname = stdout
	if hostname.startswith("gridka"):
		raise BatchelorException("hostname '" + hostname + "' seems to indicate gridka, but the wrong host")
	elif hostname == "compass-kit.gridka.de":
		return "gridka"
	elif hostname.startswith("lxplus") or hostname.endswith(".cern.ch"):
		return "lxplus"
	elif hostname.endswith(".e18.physik.tu-muenchen.de"):
		return "e18"
	elif hostname.startswith("ccage"):
		return "lyon"
	elif hostname.startswith("login") and runCommand("which llsubmit")[0] == 0:
		return "c2pap"
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
	                    "e18": [ "shortqueue", "memory", "header_file", "arch" ],
	                    "gridka": [ "queue", "project", "memory", "header_file" ],
	                    "lxplus": [ "queue", "pool", "header_file" ],
	                    "lyon": [],
	                    "local": [ "shell", "cores" ],
	                    "simulator": [ "lifetime" ] }
	filesToTest = { "gridka": [ "header_file" ],
	                "e18": [ "header_file" ],
	                "lxplus": [ "header_file" ],
	                "c2pap": [ "header_file" ],
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
			import batchelor._batchelorLxplus as batchFunctions
		elif self._system == "lyon":
			import batchelor._batchelorLyon as batchFunctions
		elif self._system == "local":
			import batchelor._batchelorLocal as batchFunctions
			batchFunctions.initialize(self._config)
		elif self._system == "simulator":
			import batchelor._batchelorSimulator as batchFunctions
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

	def submitJob(self, command, outputFile, jobName = None):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if "submitJob" in self.batchFunctions.__dict__.keys():
			_checkForSpecialCharacters(jobName)
			return self.batchFunctions.submitJob(self._config, command, outputFile, jobName)
		else:
			raise BatchelorException("not implemented")

	def submitArrayJob(self, command, outputFile, jobName = None, arrayStart = 0, arrayEnd = None, arrayStep = 1):
		if not self.initialized():
			raise BatchelorException("not initialized")
		if arrayStart is not None:
			arrayEnd = arrayStart if arrayEnd is None else arrayEnd
		try:
			int(arrayStart)
			int(arrayEnd)
			int(arrayStep)
		except ValueError:
			raise BatchelorException('One of the job array parameters is non-integer')
		if int(arrayEnd) < int(arrayStart):
			raise BatchelorException('Last job number in array is bigger than the first')
		if "submitJob" in self.batchFunctions.__dict__.keys():
			if not 'arrayStart' in inspect.getargspec(self.batchFunctions.submitJob)[0]:
				raise BatchelorException("not implemented")
			else:
				return self.batchFunctions.submitJob(self._config, command, outputFile, jobName,
				                                     arrayStart, arrayEnd, arrayStep)
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
