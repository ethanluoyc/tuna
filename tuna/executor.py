import htcondor
import time
import logging
import os

_POLL_EVERY_N_SECONDS = 5

_logger = logging.getLogger("tuna.executor")

class CondorExecutionError(Exception):
  pass

class Executor(object):
  def __init__(self):
    self.schedd = htcondor.Schedd()
    self._update_last_queried()

  def submit(self, executable="/bin/echo", arguments="hello", job_id=1, 
             experiment_name='foo', ):
    submit = htcondor.Submit(
      {"executable": executable, 
       "arguments": arguments,
       "getenv": "True",
       "output": "{}.out".format(job_id),
       "error": "{}.err".format(job_id),
       "environment": "TUNA_RUN_ID={};TUNA_EXPERIMENT_NAME={}".format(job_id, experiment_name)
       })

    with self.schedd.transaction() as txn:
      cluster_id = submit.queue(txn, count=1)
      _logger.info("submitted {}".format(cluster_id))
      return cluster_id

  def _handle_complete(self, job_spec):
    job_id = "{}.{}".format(job_spec['ClusterId'], job_spec['ProcId'])
    if int(job_spec['ExitStatus']) != 0:
      _logger.info("failed {}".format(job_id))
    else:
      _logger.info("finished {}".format(job_id))

  def _update_last_queried(self):
    self.last_queried_time = int(time.time())

  def _get_newly_completed_jobs(self):
    return self.schedd.history("CompletionDate >= %d" % self.last_queried_time, [])

  def _get_running_cluster_ids(self):
    result = self.schedd.query()
    return set([r['ClusterId'] for r in result])

  def wait_on_all(self):
    while True:
      running_ids = self._get_running_cluster_ids()
      _logger.info("running: {}".format(running_ids))
      for job in self._get_newly_completed_jobs():
        self._handle_complete(job)
      if len(running_ids) == 0:
        break
      self._update_last_queried()
      time.sleep(_POLL_EVERY_N_SECONDS)
  
  def wait_on_job(self, cluster_id):
    constraint = "ClusterId == {}".format(cluster_id)
    while True:
      running_response = self.schedd.query(constraint)
      if len(running_response) == 0:
        history_response = list(self.schedd.history(constraint, []))
        if len(history_response) == 0:
          raise ValueError("Unable to find for cluster_id == {}".format(cluster_id))
        if any([r['ExitStatus'] != 0 for r in history_response]):
          raise CondorExecutionError("Some of the processes have failed")
        break
      else:
        time.sleep(_POLL_EVERY_N_SECONDS)

if __name__ == "__main__":
  exe = Executor()
  id0 = exe.submit()
  id1 = exe.submit()
  exe.wait_on_all()
