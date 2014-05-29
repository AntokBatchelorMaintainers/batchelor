
import ConfigParser
import os.path
import subprocess


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
	elif hostname.startswith("lxplus") and hostname.endswith(".cern.ch"):
		return "lxplus"
	elif hostname.endswith(".e18.physik.tu-muenchen.de"):
		return "e18"
	elif hostname.startswith("ccage"):
		return "lyon"
	return "UNKNOWN"


def _getRealPath(path):
	return os.path.abspath(os.path.expandvars(os.path.expanduser(path)))


def checkConfig(configFileName):
	config = ConfigParser.RawConfigParser()
	if not config.read(os.path.abspath(configFileName)):
		print("ERROR: Could not read config file '" + configFileName + "'.")
		return False
	error = False
	requiredOptions = { "e18": [],
	                    "gridka": ["queue", "project", "memory", "header_file"],
						"lxplus": [],
						"lyon": [] }
	filesToTest = { "gridka": ["header_file"] }
	for section in requiredOptions.keys():
		if config.has_section(section):
			options = requiredOptions[section]
			for option in options:
				if not config.has_option(section, option):
					print("ERROR: Gridka section is missing option '" + option + "'.")
					error = True
					continue
				if section in filesToTest.keys() and option in filesToTest[section]:
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
		if not checkConfig(configFileName):
			self.bprint("Config file contains errors. Initialization failed...")
			return False
		self.bprint("Importing appropriate submodule.")
		if self._system == "gridka":
			import batchelor._batchelorGridka as batchFunctions
		elif self._system == "lxplus":
			import batchelor._batchelorLxplus as batchFunctions
		elif self._system == "e18":
			import batchelor._batchelorE18 as batchFunctions
		elif self._system == "lyon":
			import batchelor._batchelorLyon as batchFunctions
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
		return self.batchFunctions.submitJob(self._config, command, outputFile, jobName)

	def getNJobs(self, jobName = None):
		if not self.initialized():
			raise BatchelorException("not initialized")
		return self.batchFunctions.getNJobs(jobName)

	def jobStillRunning(self, jobId):
		if not self.initialized():
			raise BatchelorException("not initialized")
		return self.batchFunctions.jobStillRunning(jobId)
