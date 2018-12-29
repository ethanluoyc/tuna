import htcondor
import time
import logging
import os

_POLL_EVERY_N_SECONDS = 5

_logger = logging.getLogger("tuna.executor")

class CondorExecutionError(Exception):
    pass

class CondorSubmittedRun(object):
    def __init__(self, run_id, cluster_id, schedd):
        super(CondorSubmittedRun, self).__init__()
        self._run_id = run_id
        self._cluster_id = cluster_id
        self._schedd = schedd

    def wait(self):
        constraint = "ClusterId == {}".format(self._cluster_id)
        while True:
            running_response = self._schedd.query(constraint)
            if len(running_response) == 0:
                history_response = list(self._schedd.history(constraint, []))
                if len(history_response) == 0:
                    raise ValueError("Unable to find for cluster_id == {}".format(self._cluster_id))
                if any([r['ExitStatus'] != 0 for r in history_response]):
                    raise CondorExecutionError("Some of the processes have failed")
                break
            else:
                time.sleep(_POLL_EVERY_N_SECONDS)
    
    def get_status(self):
        constraint = "ClusterId == {}".format(self._cluster_id)
        running_response = self._schedd.query(constraint)
        if len(running_response) == 0:
            history_response = list(self._schedd.history(constraint, []))
            if len(history_response) == 0:
                raise ValueError("Unable to find for cluster_id == {}".format(self._cluster_id))
            if any([r['ExitStatus'] != 0 for r in history_response]):
                return "FAILED"
            return "SUCCESS"
        else:
            return "RUNNING"

    def cancel(self):
        raise NotImplementedError

    @property
    def run_id(self):
        return self._run_id


class CondorJobRunner(object):
    def __init__(self):
        self.schedd = htcondor.Schedd()
        self._update_last_queried()

    def submit(self, 
               executable, 
               arguments,
               experiment_name,
               run_id):
        
        submit = htcondor.Submit({
            "executable": executable, 
            "arguments": arguments,
            "getenv": "True",
            "output": "{}.out".format(run_id),
            "error": "{}.err".format(run_id),
            "log": "{}.log".format(run_id),
            "environment": "TUNA_RUN_ID={}; TUNA_EXPERIMENT_NAME={}".format(run_id, experiment_name)
            })

        with self.schedd.transaction() as txn:
            cluster_id = submit.queue(txn, count=1)
            _logger.info("submitted {}".format(cluster_id))
            return CondorSubmittedRun(run_id, cluster_id, self.schedd)

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
    pass
