import htcondor
import time
import logging
import os
import sys

_POLL_EVERY_N_SECONDS = 5

_logger = logging.getLogger("tuna.executor")

def _build_environment_str(env_vars):
    return ";".join(["{}={}".format(k, v) for k, v in env_vars.items()])

class CondorExecutionError(Exception):
    pass

class CondorSubmittedRun(object):
    def __init__(self, cluster_id, schedd):
        super(CondorSubmittedRun, self).__init__()
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
                if any([r['ExitCode'] != 0 for r in history_response]):
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
            if any([r['ExitCode'] != 0 for r in history_response]):
                return "FAILED"
            return "SUCCESS"
        else:
            return "RUNNING"

    def cancel(self):
        raise NotImplementedError


class CondorJobRunner(object):
    def __init__(self):
        self.schedd = htcondor.Schedd()
        self._update_last_queried()

    def submit(self, 
               executable, 
               arguments,
               env_vars=None,
               submit_kwargs=None,
               ):
        
        submit_kwargs = submit_kwargs.copy() if submit_kwargs else {}
        submit_kwargs.update({
            "executable": executable, 
            "arguments": arguments,
            
        })
        if env_vars:
            submit_kwargs['environment'] = _build_environment_str(env_vars)

        submit = htcondor.Submit(submit_kwargs)

        with self.schedd.transaction() as txn:
            cluster_id = submit.queue(txn, count=1)
            _logger.info("submitted {}".format(cluster_id))
            return CondorSubmittedRun(cluster_id, self.schedd)

    def _handle_complete(self, job_spec):
        job_id = "{}.{}".format(job_spec['ClusterId'], job_spec['ProcId'])
        if int(job_spec['ExitCode']) != 0:
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
                if any([int(r['ExitCode']) != 0 for r in history_response]):
                    raise CondorExecutionError("Some of the processes have failed")
                break
            else:
                time.sleep(_POLL_EVERY_N_SECONDS)


def run_condor(executable, 
               arguments,
               experiment_name,
               run_id
               ):
    # run = run_condor('./test.sh', "Hello", "world", 1)
    env = {
        "TUNA_RUN_ID": run_id,
        "TUNA_EXPERIMENT_NAME": experiment_name
    }
    runner = CondorJobRunner()
    return runner.submit(
        executable, 
        arguments, 
        env_vars=env,
        submit_kwargs={"getenv": "True"}
    )

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    runner = CondorJobRunner()
    for t in range(10):
      run = runner.submit("/bin/sh", "\"-c 'sleep {}'\"".format(t), submit_kwargs={'error': '{}.error'.format(t)})
    runner.wait_on_all()
