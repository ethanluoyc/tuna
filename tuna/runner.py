#!/usr/bin/env python

import sys
from tuna import script_runner, client
import json
import os


if __name__ == "__main__":
    [print(p) for p in sys.argv]
    fpath, fname, params = sys.argv[1], sys.argv[2], sys.argv[3][1:-1]
    params = json.loads(params.replace("\\\"", "\""))
    experiment_name, run_id = client.start_run()
    func = script_runner.import_function(fpath, fname)
    objective_value = func(params)
    
    client.log_params(experiment_name, run_id, params)
    client.log_value(experiment_name, run_id, objective_value)
