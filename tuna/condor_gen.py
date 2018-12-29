import json
import argparse
import os
import sys
from tuna.variant_generator import generate_trials

here = os.path.abspath(os.path.dirname(__file__))


def quote(s):
    return "\"'" + s + "'\""  # Quoting arguments


def escape(config):
    return config.replace("\"", "\\\"")


def to_argv(config, escape_condor=False):
    argv = []
    for k, v in config.items():
        if v is None:
            continue
        argv.append("--{}".format(k.replace("_", "-")))
        if isinstance(v, str):
            if escape_condor and isinstance(v, str):
                v = "'" + escape(v) + "'"
            argv.append(v)
        else:
            if escape_condor:
                argv.append("'" + escape(json.dumps(v)) + "'")
            else:
                argv.append(json.dumps(v))
    return argv


def create_experiments(experiments, output=None):
    """

    Experiments specification takes the following form:
    ```
    experiments = {
        'exp1': {"run": "run",
                 "entry": train,
                 "config": {
                    "alpha": grid_search([0.2, 0.4, 0.6]),
                    "beta":  grid_search([1, 2]),
                }}
    }
    ```
    """
    from tuna.script_runner import get_function_info
    if output is None:
        output = sys.stdout
    else:
        output = open(output, 'w')

    def prepare_script(name, exp):
        for trial in generate_trials(exp):
            exp_dir = os.path.abspath(
                os.path.join(trial.local_dir, name, '_'.join([trial.trainable_name, trial.experiment_tag])))
            os.makedirs(exp_dir, exist_ok=True)
            t = vars(trial).copy()
            argv = to_argv(t, True)
#            output.write"regexp(\"^(graphic)[0-9][0-9]\", TARGET.Machine)\n"
            output.write('initialdir = {}\n'.format(exp_dir))
            output.write("arguments = " + "\"" + ' '.join(argv) + "\" \nqueue\n")

    with open(os.path.join(here, 'tune.sub'), 'r') as template_file:
        output.write(template_file.read())
        output.write('\n\n')
        output.write('executable = tuna-run\n')

    for name, spec in experiments.items():
        fn_file, fn_name = get_function_info(spec['entry'])
        spec['entry'] = "{}:{}".format(fn_file, fn_name)
        prepare_script(name, spec)

    if output is not sys.stdout:
        output.close()


def create_experiments_bash(experiments, output=None):
    from tuna.script_runner import get_function_info
    from tuna.utils import quote

    if output is None:
        output = sys.stdout
    else:
        output = open(output, 'w')

    def prepare_script(name, exp):
        for trial in generate_trials(exp):
            exp_dir = os.path.abspath(
                os.path.join(trial.local_dir, name, '_'.join([trial.trainable_name, trial.experiment_tag])))
            os.makedirs(exp_dir, exist_ok=True)
            t = vars(trial).copy()
            argv = to_argv(t, False)
            runner_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'runner.py')
            output.write("python {} ".format(runner_file)
                         + ' '.join([quote(a) for a in argv]) + "\n")

    for name, spec in experiments.items():
        fn_file, fn_name = get_function_info(spec['entry'])
        spec['entry'] = "{}:{}".format(fn_file, fn_name)
        prepare_script(name, spec)

    if output is not sys.stdout:
        output.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output')
