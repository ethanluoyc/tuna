import subprocess
import time
import os

class LocalExecutor(object):
    """Serial executor based on subprocess. Used for testing"""
    def __init__(self):
        self._running_procs = []

    def submit(self, executable, arguments, job_id=1, experiment_name='foo'):
        new_env = os.environ.copy()
        new_env['TUNA_RUN_ID'] = str(job_id)
        new_env['TUNA_EXPERIMENT_NAME'] = experiment_name
        proc = subprocess.Popen(
            [executable] + arguments, env=new_env)
        self._running_procs.append(proc)
        return proc

    def wait_on_job(self, job):
        job.wait()

    def wait_on_all(self):
        while len(self._running_procs) > 0:
            proc = self._running_procs.pop()
            proc.wait()

if __name__ == "__main__":
    exc = LocalExecutor()
    job0 = exc.submit('/bin/sleep', ['3'])
    job1 = exc.submit('/bin/sleep', ['3'])
    exc.wait_on_all()
