import argparse
import configparser
import os

from jinja2 import Environment, FileSystemLoader, meta
import jinja2schema


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
    parser.add_argument("--secrets", "-s", action="store")
    parser.add_argument("--config", "-c", action="store")
    parser.add_argument("--database", "-d", action="store")
    parser.add_argument("--flow", "-f", action="store")

    return parser.parse_args()


class FlowStepParameters(object):
    def __init__(self):
        self.step_name = ""
        self.step_parameters = dict()
        self.variables_per_parameter = dict()


class RenderingContext(object):
    def __init__(self):
        self.config_file_name = None
        self.secrets_file_name = None
        self.database_name = None
        self.flow_name = None

        self.secrets_parser = None
        self.config_parser = None

        self.flow_human_readable_name = None
        self.flow_steps = None
        self.flow_steps_parameters = []


class YabtoolApplication(object):
    def __init__(self, logger):
        self.logger = logger
        self._rendering_context = None

    def run(self):
        args = get_cli_args()

        self._rendering_context = RenderingContext()

        self._rendering_context.config_file_name = self._get_config_file_name(args)
        self.logger.debug("config_file_name: '{}'".format(self._rendering_context.config_file_name))
        if (not self._rendering_context.config_file_name) or (not os.path.exists(self._rendering_context.config_file_name)):
            self.logger.error("can't find config file '{}'".format(self._rendering_context.config_file_name))
            return False
        self._rendering_context.config_parser = configparser.ConfigParser()
        self._rendering_context.config_parser.read(self._rendering_context.config_file_name)

        self._rendering_context.secrets_file_name = self._get_secrets_file_name(args)
        self.logger.debug("config_file_name: '{}'".format(self._rendering_context.secrets_file_name))
        if (not self._rendering_context.secrets_file_name) or (not os.path.exists(self._rendering_context.secrets_file_name)):
            self.logger.error("can't find file with secrets '{}'".format(self._rendering_context.secrets_file_name))
            return False
        self._rendering_context.secrets_parser = configparser.ConfigParser()
        self._rendering_context.secrets_parser.read(self._rendering_context.secrets_file_name)

        self._rendering_context.database_name = self._get_database_name(args)
        self.logger.debug("database name: '{}'".format(self._rendering_context.database_name))
        if not self._rendering_context.database_name:
            self.logger.error("no database specified nor in command line nor in configuration file with secrets")
            return False

        self._rendering_context.flow_name = self._get_flow_name(args)
        self.logger.debug("flow name: '{}'".format(self._rendering_context.flow_name))
        if not self._rendering_context.flow_name:
            self.logger.error("no database specified nor in command line nor in configuration file with secrets")
            return False

        if not self._validate_inputs():
            self.logger.error("input validation failed")
            return False

        return True

    def _get_config_file_name(self, args):
        config_file_name = args.config

        if not config_file_name:
            config_file_name = DEFAULT_CONFIG_RELATIVE_NAME
            src_path = os.path.abspath(os.path.dirname(__file__))
            assert os.path.exists(src_path)
            self.logger.debug("src_path: '{}'".format(src_path))

            config_file_name = os.path.join(src_path, config_file_name)
            config_file_name = os.path.abspath(config_file_name)
            config_file_name = os.path.normpath(config_file_name)
            assert os.path.exists(config_file_name)
        else:
            config_file_name = os.path.abspath(config_file_name)
            config_file_name = os.path.normpath(config_file_name)

        return config_file_name

    @staticmethod
    def _get_secrets_file_name(args):
        secrets_file_name = args.secrets
        secrets_file_name = os.path.abspath(secrets_file_name)
        secrets_file_name = os.path.normpath(secrets_file_name)

        return secrets_file_name

    def _get_database_name(self, args):
        if args.database:
            return args.database

        assert self._rendering_context.secrets_parser
        return self._rendering_context.secrets_parser.get("defaults", "database", fallback="abc")

    def _get_flow_name(self, args):
        if args.flow:
            return args.flow

        assert self._rendering_context.secrets_parser
        return self._rendering_context.secrets_parser.get("defaults", "flow", fallback="abc")

    def _validate_inputs(self):
        config_parser = self._rendering_context.config_parser

        self.logger.debug("available sections: {}".format(config_parser.sections()))
        flow_section_name = self._compile_flow_section_name(self._rendering_context.flow_name)
        self.logger.debug("flow_section_name: '{}'".format(flow_section_name))
        if not config_parser.has_section(flow_section_name):
            self.logger.error("no flow with name '{}' in configuration file".format(self._rendering_context.flow_name))
            return False

        self._rendering_context.flow_human_readable_name = config_parser.get("flow_section_name", "name", fallback=None)
        self._rendering_context.flow_steps = self._get_steps_for_flow(flow_section_name)
        self.logger.debug("flow steps: {}".format(self._rendering_context.flow_steps))

        if not self._load_parameters_for_steps():
            return False

        return True

    def _get_steps_for_flow(self, flow_section_name):
        steps = self._rendering_context.config_parser.get(flow_section_name, "steps", fallback="")
        if steps:
            steps = str(steps).strip().split(",")

        steps = [str(item).strip() for item in steps if str(item).strip()]

        return steps

    def _load_parameters_for_steps(self):
        config_parser = self._rendering_context.config_parser

        for step_name in self._rendering_context.flow_steps:
            step_config = FlowStepParameters()

            step_config.step_name = step_name
            step_config.step_parameters = dict()

            step_config_section_name = self._compile_flow_step_template_section_name(self._rendering_context.flow_name, step_name)
            self.logger.debug("loading parameters for '{}'".format(step_config_section_name))

            if config_parser.has_section(step_config_section_name):
                data = config_parser.items(step_config_section_name)
                step_config.step_parameters = {k: v for k, v in data}

                step_config.variables_per_parameter = self._get_parameters_per_key(step_config.step_parameters)
                if step_config.variables_per_parameter is None:
                    return False

            self._rendering_context.flow_steps_parameters.append(step_config)

        return True

    def _get_parameters_per_key(self, parameters):
        res = {}
        allowed_types = ["<scalar>", "<string>", "<number>", "<boolean>"]
        for parameter_name, parameter_template_value in parameters.items():
            self.logger.debug("going to extract parameters name for '{}'".format(parameter_template_value))

            # TODO: catch exceptions
            variables = jinja2schema.infer(parameter_template_value)
            self.logger.debug("variables: '{}'".format(variables))

            required_parameters = list()
            for value_name, value_type in variables.items():
                if repr(value_type) not in allowed_types:
                    self.logger.error("value type '{}' for step '{}' is unsupported".format(value_name, parameter_name))
                    return None

                required_parameters.append(value_name)

            res[parameter_name] = sorted(required_parameters)
            self.logger.debug("parameters for '{}' is: {}".format(parameter_name, required_parameters))

        return res

    @staticmethod
    def _compile_flow_step_template_section_name(flow_name, step_name):
        return "{}-{}-config-template".format(flow_name, step_name)

    @staticmethod
    def _compile_flow_section_name(flow_name):
        return "{}-flow".format(flow_name)
