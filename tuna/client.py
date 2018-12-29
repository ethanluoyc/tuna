import os
import json

_RESULT_FILE = 'RESULT'
_PARAMS_FILE = 'PARAMS'
_RUNS_PATH = 'runs'

def _get_experiment_dir(experiment_name):
    return os.path.join('exps', experiment_name)

def _get_run_dir(experiment_name, run_id):
    return os.path.join(_get_experiment_dir(experiment_name), _RUNS_PATH, '{}'.format(run_id))

def start_run():
    experiment_name = os.environ.get('TUNA_EXPERIMENT_NAME')
    run_id = os.environ.get('TUNA_RUN_ID')
    os.makedirs(_get_run_dir(experiment_name, run_id), exist_ok=True)
    return experiment_name, run_id

def log_params(experiment_name, run_id, params):
    rundir = os.path.join(_get_run_dir(experiment_name, run_id))
    with open(os.path.join(rundir, _PARAMS_FILE), 'wt') as outf:
        json.dump(params, outf)

def log_value(experiment_name, run_id, value):
    rundir = os.path.join(_get_run_dir(experiment_name, run_id))
    os.makedirs(rundir, exist_ok=True)
    with open(os.path.join(rundir, _RESULT_FILE), 'wt') as outf:
        outf.write('{}'.format(value))

def get_params(experiment_name, run_id):
    rundir = os.path.join(_get_run_dir(experiment_name, run_id))
    with open(os.path.join(rundir, _PARAMS_FILE), 'rt') as infile:
        return json.load(infile)

def get_value(experiment_name, run_id):
    rundir = os.path.join(_get_run_dir(experiment_name, run_id))
    with open(os.path.join(rundir, _RESULT_FILE), 'rt') as infile:
        return float(infile.read())

def _get_run_ids(experiment_name):
    subdirs = os.listdir(os.path.join(_get_experiment_dir(experiment_name), _RUNS_PATH))
    return [int(s) for s in subdirs]

def read_metrics(experiment_name):
    runs = {}
    for job_id in _get_run_ids(experiment_name):
        params = get_params(experiment_name, job_id)
        runs[job_id] = (params, get_value(experiment_name, job_id))
    return runs
