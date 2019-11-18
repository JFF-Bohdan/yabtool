import argparse
import os
import shutil
import sys

import loguru
from loguru._defaults import LOGURU_FORMAT
from yabtool.version import __version__

from .yabtool_flow_orchestrator import YabtoolFlowOrchestrator


def get_cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version="%(prog)s {}".format(__version__)
    )

    parser.add_argument(
        "--log-level",
        "-l",
        action="store",
        default="DEBUG",
        help="Specify log level (DEBUG, INFO, ...)"
    )

    parser.add_argument(
        "--disable-voting",
        "-k",
        action="store_true",
        help="Allow voting for flow execution skipping"
    )

    parser.add_argument(
        "--secrets",
        "-s",
        action="store",
        help="Path to file with secrets"
    )

    parser.add_argument(
        "--config",
        "-c",
        action="store",
        help="Path to main configuration file"
    )

    parser.add_argument(
        "--dry-run",
        "-y",
        action="store_true",
        default=None,
        help="Perform dry run only"
    )

    parser.add_argument(
        "--target",
        "-d",
        action="store",
        help="Specify target in secret file (overrides default in secrets file if specified)"
    )

    parser.add_argument(
        "--temporary-folder",
        "-t",
        action="store",
        help="Path to temporary folder that will be used by yabtool"
    )

    parser.add_argument(
        "--flow",
        "-f",
        action="store",
        help="Required flow name"
    )

    return parser.parse_known_args()


class YabtoolApplication(object):
    def __init__(self):
        self.logger = None
        self.rendering_context = None

    def run(self):
        args, unknown_args = get_cli_args()
        self._initialize_logger(args)
        self.logger.debug("unknown command line arguments: {}".format(unknown_args))

        flow_orchestrator = YabtoolFlowOrchestrator(self.logger)

        if args.disable_voting:
            flow_orchestrator.skip_voting_enabled = False

        try:
            flow_orchestrator.initialize(args, unknown_args)
            flow_name = flow_orchestrator.flow_name

            if not args.dry_run:
                self.logger.info("flow '{}' started".format(flow_name))
                flow_orchestrator.run()
            else:
                self.logger.info("dry run for flow '{}' performed, cleaning up".format(flow_name))
        finally:
            folder_name = flow_orchestrator.rendering_context.temporary_folder
            if flow_orchestrator.rendering_context.remove_temporary_folder and folder_name:
                if folder_name and os.path.exists(folder_name) and os.path.isdir(folder_name):
                    self.logger.info("going to remove temporary folder: {}".format(folder_name))
                    self._remove_temporary_folder(folder_name)

            else:
                self.logger.info("output folder removal disabled. folder name: '{}'".format(folder_name))

        return True

    def _initialize_logger(self, args):
        self.logger = loguru.logger
        self.logger.remove()
        self.logger.add(sys.stdout, format=LOGURU_FORMAT, level=args.log_level)

    def _remove_temporary_folder(self, folder_name):
        try:
            shutil.rmtree(folder_name)
        except Exception as e:
            self.logger.error("error removing folder '{}': {}".format(folder_name, e))
