import argparse
import os

from yabtool.version import __version__


DEFAULT_CONFIG_RELATIVE_NAME = "../config/config.ini"


def dir_path(path):
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError("readable_dir:{path} is not a valid path".format(path=path))


def get_cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--version", "-v", action="version", version="%(prog)s {}".format(__version__))
    parser.add_argument("--config", "-c", action="store")
    parser.add_argument("--secrets", "-c", action="store")

    return parser.parse_args()


class YabtoolApplication(object):
    def __init__(self, logger):
        self.logger = logger

    def run(self):
        pass
