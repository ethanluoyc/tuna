
import json
from tuna.executor import Executor
from tuna.script_runner import get_function_info, import_function
from tuna import client
import os
import time

_here = os.path.dirname(os.path.abspath(__file__))
_RUNNER_FILE = os.path.join(_here, 'runner.py')

def run(experiment_name, f, parameters_set):
    from tuna.condor_gen import quote, escape
    fpath, fname = get_function_info(f)
    exc = Executor()
    job_ids = []
    for i, param in enumerate(parameters_set):
        exc.submit(_RUNNER_FILE, quote(" ".join([fpath, fname, escape(json.dumps(param))])),
            experiment_name=experiment_name,
            job_id=i)
        job_ids.append(i)
    exc.wait_on_all()
    time.sleep(10)
    values = client.read_metrics(experiment_name)
    print(values)


def objective(params):
    print(params)
    return (params['a'] - 1) * (params['a'] - 1) / 2

if __name__ == "__main__":
    run('hello-world', objective, [{'a': 1}, {'a': 2}])
