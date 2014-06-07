#!/usr/bin/env python

import argparse
import sys

import batchelor


if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="check a batchelor configuration file")
	parser.add_argument("configFile", type=str, metavar="configFile", help="configuration file to be checked")
	args = parser.parse_args()

	system = batchelor.detectSystem()
	if system == "UNKNOWN":
		print("Could not detect system, checking for all files...")
		system = ""
	else:
		print("System '" + system + "' detected, checking only appropriate files...")

	if batchelor.checkConfig(args.configFile, system):
		print("Config file is valid.")
	else:
		print("Config file contains errors.")
		sys.exit(1)
