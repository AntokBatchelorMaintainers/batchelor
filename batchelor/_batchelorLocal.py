
import multiprocessing
import os
import subprocess
import tempfile

import batchelor


class Job:

	def __init__(self, jobId, command, outputFile, jobName):
		self.jobId = jobId
		self.command = command
		self.outputFile = outputFile
		self.jobName = jobName
		self.running = False


class Worker(multiprocessing.Process):

	def __init__(self, shell, queue, guard, jobs):
		multiprocessing.Process.__init__(self)
		self.shell = shell
		self.queue = queue
		self.guard = guard
		self.jobs = jobs

	def run(self):
		while True:
			jobId = self.queue.get()
			with guard:
				for i in range(len(self.jobs)):
					if jobs[i].jobId == jobId:
						break
				if jobs[i].jobId != jobId:
					continue # might actually happen if a
					         # job is deleted
				jobs[i].running = True
				outputFile = jobs[i].outputFile
				command = jobs[i].command
			cmdFile = tempfile.NamedTemporaryFile(delete = False)
			for line in command:
				cmdFile.write(line)
			cmdFile.close()

			with open(outputFile, "w") as logFile:
				subprocess.call([self.shell, cmdFile.name], stdout=logFile, stderr=subprocess.STDOUT)

			os.unlink(cmdFile.name)
			with guard:
				for i in range(len(self.jobs)):
					if jobs[i].jobId == jobId:
						break
				if jobs[i].jobId != jobId:
					raise batchelor.BatchelorException("Job ID {0} finished, but already removed from list of jobs.".format(jobId))
				del jobs[i]
			self.queue.task_done()


manager = multiprocessing.Manager()
guard = manager.Lock()
queue = manager.Queue()
jobs = manager.list()
aux = manager.list([0])


def initialize(config):
	cores = int(config.get(submoduleIdentifier(), "cores"))
	if cores == 0:
		cores = multiprocessing.cpu_count()

	shell = config.get(submoduleIdentifier(), "shell")

	for i in range(cores):
		worker = Worker(shell, queue, guard, jobs)
		worker.start()


def submoduleIdentifier():
	return "local"


def submitJob(config, command, outputFile, jobName):
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
	for jobId in jobIds:
		with guard:
			for i in range(len(jobs)):
				if jobs[i].jobId == jobId:
					break
			if jobs[i].jobId != jobId:
				continue
			if jobs[i].running == True:
				continue
			del jobs[i]
	return True
