from __future__ import absolute_import

from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import object
import multiprocessing
import os
import sys
from queue import Queue, Empty
import subprocess
import tempfile
import threading

import batchelor
from ._job import JobStatus


class Job(object):

	def __init__(self, jobId, command, outputFile, jobName):
		self.jobId = jobId
		self.command = command
		self.outputFile = outputFile
		self.jobName = jobName
		self.running = False
		self.runningProcess = None


class Worker(threading.Thread):

	def __init__(self, shell):
		threading.Thread.__init__(self)
		self.shell = shell

	def run(self):
		while True:
			try:
				jobId = queue.get(timeout = 2)
			except Empty:
				with guard:
					if aux[1]:
						break
					else:
						continue

			with guard:
				knownJobIds = [ job.jobId for job in jobs ]
				if not jobId in knownJobIds:
					# might actually happen if a job is deleted
					continue
				i = knownJobIds.index(jobId)
				jobs[i].running = True
				outputFile = jobs[i].outputFile
				command = jobs[i].command
				cmdFile = tempfile.NamedTemporaryFile(delete = False)
				for line in command:
					cmdFile.write(line.encode())
				cmdFile.close()
				
				logFile = open(outputFile, "w")
				p = subprocess.Popen([self.shell, cmdFile.name], stdout=logFile, stderr=subprocess.STDOUT, encoding='utf-8', preexec_fn=lambda : os.setpgid(0, 0))
				jobs[i].runningProcess = p

			p.wait()

			logFile.close()
			os.unlink(cmdFile.name)
			with guard:
				knownJobIds = [ job.jobId for job in jobs ]
				if not jobId in knownJobIds:
					raise batchelor.BatchelorException("Job ID {0} finished, but already removed from list of jobs.".format(jobId))
				i = knownJobIds.index(jobId)
				del jobs[i]
			queue.task_done()


workers = []
guard = threading.Lock()
queue = Queue()
jobs = []
aux = [0, False]


def initialize(config):
	cores = int(config.get(submoduleIdentifier(), "cores"))
	if cores == 0:
		cores = multiprocessing.cpu_count()

	shell = config.get(submoduleIdentifier(), "shell")

	# in case the 'initialize' function of batchelor is called multiple
	# times, the number of workers might pile up, so only make sure that
	# the number of existing workers is equal to the current desired
	# setting.
	newcores = 0 if len(workers) >= cores else cores - len(workers)
	for i in range(newcores):
		worker = Worker(shell)
		worker.start()
		workers.append(worker)


def shutdown():
	global workers
	global aux
	global jobs
	# signal processes to stop after all jobs have been finished from queue
	with guard:
		aux[1] = True

	for worker in workers:
		worker.join()

	workers = []
	jobs = []
	aux = [0, False]


def submoduleIdentifier():
	return "local"


def submitJob(config, command, outputFile, jobName, wd = None):
	if wd:
		command = "cd '{0}'; {1}".format(wd, command)

	with guard:
		aux[0] += 1
		jobId = aux[0]
		jobs.append(Job(jobId, command, outputFile, jobName))
	queue.put(jobId)
	return jobId


def submitJobs(config, newJobs):
	jobIds = []
	with guard:
		for job in newJobs:
			aux[0] += 1
			jobId = aux[0]
			command = job[0]
			outputFile = job[1]
			jobName = None
			if len(job) == 3:
				jobName = job[2]

			jobs.append(Job(jobId, command, outputFile, jobName))
			queue.put(jobId)
			jobIds.append(jobId)
	return jobIds


def getListOfActiveJobs(jobName):
	with guard:
		return [ job.jobId for job in jobs ]


def getNActiveJobs(jobName):
	with guard:
		return len(jobs)


def jobStillRunning(jobId):
	with guard:
		for i in range(len(jobs)):
			if jobs[i].jobId == jobId:
				return True
	return False


def getListOfErrorJobs(jobName):
	return []


def resetErrorJobs(jobName):
	return True


def deleteErrorJobs(jobName):
	return True


def deleteJobs(jobIds):
	with guard:
		for jobId in jobIds:
			knownJobIds = [ job.jobId for job in jobs ]
			if not jobId in knownJobIds:
				continue
			i = knownJobIds.index(jobId)
			if jobs[i].running == True:
				os.killpg(os.getpgid(jobs[i].runningProcess.pid), subprocess.signal.SIGTERM)
				continue # need to continue, because the worker removes the job
			del jobs[i]
	return True


def getListOfJobStates(jobIds, username):
	jobStates = []

	with guard:
		for i in range(len(jobs)):
			if not jobIds or jobs[i].jobId in jobIds:
				jobStates.append(JobStatus(jobs[i].jobId, JobStatus.kRunning if jobs[i].running else JobStatus.kWaiting))

	return jobStates
