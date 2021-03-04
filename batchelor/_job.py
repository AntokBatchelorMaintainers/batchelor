'''
Created on Oct 23, 2014

'''

from builtins import range
from builtins import object
class JobStatus(object):
    
    
    # Status
    (kRunning, kWaiting, kError, kUnknown, kTransmitting, kDeletion ) = list(range(6));


    def __init__(self, job_id, status = None):
        
        self._id = int(job_id);
        
        self._status = status;
        
        self._status_name = None;
        
        self._cpu_time = { -1: 0.0};
        self._memory_usage = {-1: 0.0};

    def setStatus(self, value, name = None):
        self._status = int(value)
        if name != None:
            self._status_name = name;


    def setCpuTime(self, value, task_id):
        self._cpu_time[-1] += float(value);
        self._cpu_time[task_id] = float(value);


    def setMemoryUsage(self, value, task_id):
        self._memory_usage[-1] += float(value);
        self._memory_usage[task_id] = float(value);


    def getId(self):
        '''
        @return: Id of the job
        @rtype: int
        '''
        return self._id


    def getStatus(self):
        '''
        @return: Status of the job
        @rtype: int
        '''
        return self._status


    def getCpuTime(self, task_id = -1):
        '''
        @return: CPU time of the job in hours
        @rtype: float
        '''
        return self._cpu_time[task_id]


    def getMemoryUsage(self, task_id = -1):
        '''
        @return: Memory usage of the job in GB
        @rtype: float
        '''
        return self._memory_usage[task_id];
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        