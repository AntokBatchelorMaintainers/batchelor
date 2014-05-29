
import ConfigParser
import os.path
import subprocess


def BatchelorException(Exception):

	def __init__(self, value):
		self.value = value

	def __str__(self):
		return repr(self.value)


def runCommand(commandString):
	commandString = "errHandler() { (( errcount++ )); }; trap errHandler ERR\n" + commandString.rstrip('\n') + "\nexit $errcount"
	process = subprocess.Popen(commandString,
	                           shell=True,
	                           stdout=subprocess.PIPE,
	                           stderr=subprocess.STDOUT,
	                           executable="/bin/bash")
	(runCommand.lastStdout, runCommand.lastStderr) = process.communicate()
	if runCommand.lastStdout:
		runCommand.lastStdout = runCommand.lastStdout.rstrip(' \n')
	if runCommand.lastStderr:
		runCommand.lastStderr = runCommand.lastStderr.rstrip(' \n')
	errorCode = process.returncode
	if errorCode != 0:
		return False
	else:
		return True
runCommand.lastStdout = ""
runCommand.lastStderr = ""


def detectSystem():
	if not runCommand("hostname"):
		raise BatchelorException("runCommand(\"hostname\") failed")
	hostname = runCommand.lastStdout.rstrip(' \n')
	if hostname.startswith("gridka"):
		raise BatchelorException("Hostname '" + hostname + "' seems to indicate gridka, but the wrong host")
	elif hostname == "compass-kit.gridka.de":
		return "gridka"
	elif hostname.startswith("lxplus") and hostname.endswith(".cern.ch"):
		return "lxplus"
	elif hostname.endswith(".e18.physik.tu-muenchen.de"):
		return "e18"
	elif hostname.startswith("ccage"):
		return "lyon"
	return "UNKNOWN"


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

	def initialize(self, configFileName):
		self.bprint("Initializing...")
		if not self._config.read(os.path.abspath(configFileName)):
			self.bprint("Could not read config file '" + configFileName + "'. Initialization failed...")
			return False
		self._system = detectSystem()
		if self._system == "UNKNOWN":
			self.bprint("Could not determine on which system we are. Initialization failed...")
			return False
		self.bprint("Detected system '" + self._system + "'.")
		if not self._config.has_section(self._system):
			self.bprint("Could not find section describing '" + self._system +
			            "' in config file '" + configFileName + "'. Initialization failed...")
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
