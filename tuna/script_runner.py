from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import importlib
import os
import sys
import time
import threading
import traceback

from tuna.variant_generator import TuneError, TrainingResult


def import_function(file_path, function_name):
    # strong assumption here that we're in a new process
    file_path = os.path.expanduser(file_path)
    sys.path.insert(0, os.path.dirname(file_path))
    if hasattr(importlib, "util"):
        # Python 3.4+
        spec = importlib.util.spec_from_file_location(
            "external_file", file_path)
        external_file = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(external_file)
    elif hasattr(importlib, "machinery"):
        # Python 3.3
        from importlib.machinery import SourceFileLoader
        external_file = SourceFileLoader(
            "external_file", file_path).load_module()
    else:
        # Python 2.x
        import imp
        external_file = imp.load_source("external_file", file_path)
    if not external_file:
        raise TuneError("Unable to import file at {}".format(file_path))
    return getattr(external_file, function_name)


def get_function_info(f):
    import inspect
    # m = inspect.getmodule(f)
    return inspect.getfile(f), f.__name__


class TrainingContext(object):
    def __init__(self, experiment_dir, config):
        self.experiment_dir = experiment_dir


def run(args):
    logdir = os.path.abspath(
        os.path.join(args.local_dir,
                     '_'.join([args.trainable_name, args.experiment_tag])))
    ctx = TrainingContext(logdir, args.config)
    train_func = import_function(*args.entry.split(':'))
    train_func(ctx)
