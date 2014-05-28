
import ConfigParser
import os.path

class Batchelor:

	def __init__(self):
		self._config = ConfigParser.RawConfigParser()

	def initialize(self, configFileName):
		if not self._config.read(os.path.abspath(configFileName)):
			return False
		return True
