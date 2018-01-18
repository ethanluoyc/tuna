import json
import argparse


def quote(s):
    return ("\"'" + s + "'\"") # Quoting arguments


def escape(config):
    return config.replace("\"", "\"\"")


class LazyCondorRunner(object):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output')