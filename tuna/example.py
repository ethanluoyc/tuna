import json
from tuna.local_executor import LocalExecutor
from tuna.executor import Executor
from tuna.script_runner import get_function_info, import_function
from tuna import client
import os
from hyperopt import hp
import htcondor
import hyperopt
import classad

_here = os.path.dirname(os.path.abspath(__file__))
_RUNNER_FILE = os.path.join(_here, 'runner.py')

run_counter = 0
def run(experiment_name, f, space):
    fpath, fname = get_function_info(f)
    exc = Executor()
    from tuna.condor_gen import quote, escape
    
    def eval(param):
        print(param)
        global run_counter
        run_id = run_counter
        run_counter += 1
        def p(s):
            return (escape(s))
        arguments = ' '.join(list(map(p, [fpath, fname, "'" + json.dumps(param, separators=(',',':')) + "'"])))
        proc = exc.submit(_RUNNER_FILE, arguments,
            experiment_name=experiment_name,
            job_id=run_id)
        exc.wait_on_job(proc)
        return client.get_value(experiment_name, run_id)
    
    best = hyperopt.fmin(eval, space, algo=hyperopt.tpe.suggest, max_evals=3)
    print(best)


def objective(params):
    print(params)
    return (params['a']) * (params['a']) / 2

if __name__ == "__main__":
    space = {'a': hp.uniform('x', -10, 10)}
    run('hello-world', objective, space)
