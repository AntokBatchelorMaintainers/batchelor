#!/usr/bin/env python

import argparse
import os
import sys

import batchelor

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="submit jobs from list")
	parser.add_argument("batchelorConfigFileName", type=str, metavar="batchelorConfigFileName", help="config file for batchlor")
	parser.add_argument("list", type=str, metavar="commandList", help="list of commands to submit")
	parser.add_argument("jobOutputDirectory", type=str, metavar="jobOutputDirectory", help="directory to store the jobs' output")
	parser.add_argument("--job-name", type=str, metavar="jobName", dest="jobName", help="job name to use in submission")
	parser.add_argument("--simulate", action="store_true", help="do not actually submit anything")
	args = parser.parse_args()

	commandList = os.path.abspath(args.list)
	if not os.path.isfile(commandList):
		print("ERROR: command list '" + commandList + "' not valid. Aborting...")
		sys.exit(1)

	jobOutputDir = os.path.abspath(args.jobOutputDirectory)
	if not os.path.isdir(jobOutputDir):
		print("ERROR: job output directory '" + jobOutputDir + "' not found. Aborting...")
		sys.exit(1)

	print("INFO: using command list '" + commandList + "'.")
	print("INFO: using job ouptut directory '" + jobOutputDir + "'.")

	print("INFO: initializing batchelor...")
	batch = batchelor.Batchelor()
	if not batch.initialize(os.path.abspath(args.batchelorConfigFileName)):
		print("ERROR: could not initialize batchelor. Aborting...")
		sys.exit(1)
	print("INFO: batchelor initialized.")

	print("INFO: preparing job submission")
	submissionArgs = []
	with open(commandList, 'r') as commands:
		jobCounter = 1
		for line in commands:
			line = line[:-1]
			jobOut = jobOutputDir + "/"
			if args.jobName:
				jobOut += args.jobName + "_" + str(jobCounter)
			else:
				jobOut += str(jobCounter)
			jobOut += ".log"
			submissionArgs.append([line, jobOut, args.jobName])
			jobCounter += 1
			if args.simulate:
				print("DEBUG: would submit '" + line + "'.")

	while submissionArgs:

		print("INFO: job submission prepared. Submitting now...")
		jobIds = []
		if args.simulate:
			jobIds = range(len(submissionArgs))
		else:
			jobIds = batch.submitJobs(submissionArgs)
		print("INFO: jobs submitted, checking for failed submissions...")

		if not len(jobIds) == len(submissionArgs):
			print("ERROR: mismatch between submitted jobs (" + str(len(submissionArgs)) + ") and returned job ids (" + str(len(jobIds)) + "). Aborting...")
			sys.exit(1)

		newSubmissionArgs = []
		submittedJobs = 0
		for i in range(len(jobIds)):
			if jobIds[i] < 0:
				newSubmissionArgs.append(submissionArgs[i])
			else:
				submittedJobs += 1
		print("INFO: " + str(submittedJobs) + " successful job submissions, " + str(len(newSubmissionArgs)) + " have to be re-submitted.")

		if args.simulate:
			submissionArgs = []
		else:
			submissionArgs = newSubmissionArgs

	print("INFO: all jobs submitted.")
