import argparse
import codecs
import datetime
import os
import uuid

import jinja2schema
from jinja2 import BaseLoader, Environment, StrictUndefined
from yaml import safe_load

from yabtool.version import __version__

from .supported_steps import create_steps_factory

DEFAULT_CONFIG_RELATIVE_NAME = "../config/config.yaml"


def dir_path(path):
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError(
            "readable_dir:{path} is not a valid path".format(path=path)
        )


def get_cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--version", "-v", action="version", version="%(prog)s {}".format(__version__)
    )
    parser.add_argument("--secrets", "-s", action="store")
    parser.add_argument("--config", "-c", action="store")
    parser.add_argument("--database", "-d", action="store")
    parser.add_argument("--temporary_folder", "-t", action="store")
    parser.add_argument("--flow", "-f", action="store")

    return parser.parse_args()


def jinja2_custom_filter_extract_year_four_digits(value):
    return "YY"


def jinja2_custom_filter_extract_month_two_digits(value):
    return "MM"


def jinja2_custom_filter_extract_day_two_digits(value):
    return "DD"


class RenderingContext(object):
    def __init__(self):
        self.config_file_name = None
        self.secrets_file_name = None

        self.config_context = dict()
        self.secrets_context = dict()
        self.database_name = None
        self.flow_name = None

        self.basic_values = dict()
        self.previous_steps_values = list()
        self.temporary_folder = None

    def to_context(self):
        res = self.basic_values
        for item in self.previous_steps_values:
            res = {**res, **item}

        return res


class ConfigurationValidationException(BaseException):
    pass


class RenderingContextInitializer(object):
    def __init__(self, logger):
        self._rendering_context = RenderingContext()
        self.logger = logger
        self._steps_factory = None
        self._backup_start_timestamp = datetime.datetime.utcnow()

    def initialize(self, args):
        self._rendering_context.config_file_name = self._get_config_file_name(args)
        self.logger.debug(
            "config_file_name: '{}'".format(self._rendering_context.config_file_name)
        )
        if not self._rendering_context.config_file_name:
            raise ConfigurationValidationException("No configuration file specified")

        if not os.path.exists(self._rendering_context.config_file_name):
            raise ConfigurationValidationException(
                "Configuration file does not exists. Path: '{}'".format(
                    self._rendering_context.config_file_name
                )
            )

        self._rendering_context.config_context = self._load_yaml_file(
            self._rendering_context.config_file_name
        )

        self._rendering_context.secrets_file_name = self._get_secrets_file_name(args)
        if not self._rendering_context.secrets_file_name:
            raise ConfigurationValidationException(
                "Secrets file path doesn't specified"
            )

        if not os.path.exists(self._rendering_context.secrets_file_name):
            raise ConfigurationValidationException(
                "Secrets file does not exists. Path: '{}'".format(
                    self._rendering_context.secrets_file_name
                )
            )

        self._rendering_context.secrets_context = self._load_yaml_file(
            self._rendering_context.secrets_file_name
        )

        self._rendering_context.database_name = self._get_database_name(args)
        self.logger.debug(
            "database_name: '{}'".format(self._rendering_context.database_name)
        )

        self._rendering_context.flow_name = self._get_flow_name(args)
        self.logger.debug("flow_name: '{}'".format(self._rendering_context.flow_name))

        self._rendering_context.temporary_folder = self._get_temporary_folder(args)

        self._rendering_context.temporary_folder = os.path.join(
            self._rendering_context.temporary_folder,
            self._create_folder_name_for_execution(),
        )

        os.makedirs(self._rendering_context.temporary_folder)

        self.logger.debug(
            "temporary_folder: '{}'".format(self._rendering_context.temporary_folder)
        )

        self._steps_factory = create_steps_factory()

        self._rendering_context.basic_values = self._init_basic_values()
        self.logger.debug(
            "basic_values: {}".format(self._rendering_context.basic_values)
        )
        self._validate_steps_context()

        return True

    @staticmethod
    def _load_yaml_file(file_name, codec="utf-8"):
        with codecs.open(file_name, "r", codec) as input_file:
            return safe_load(input_file.read())

    @staticmethod
    def _get_secrets_file_name(args):
        # TODO: use me
        secrets_file_name = args.secrets
        secrets_file_name = os.path.abspath(secrets_file_name)
        secrets_file_name = os.path.normpath(secrets_file_name)

        return secrets_file_name

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

    def _init_basic_values(self):
        res = dict()

        res["main_database_name"] = self._rendering_context.database_name
        res["week_day_short_name"] = self._backup_start_timestamp.strftime("%a")
        res["week_number"] = self._backup_start_timestamp.strftime("%U")
        res["month_short_name"] = self._backup_start_timestamp.strftime("%b")
        res["backup_start_timestamp"] = self._backup_start_timestamp
        res["flow_name"] = self._rendering_context.flow_name
        res["yabtool_exec_folder"] = self._rendering_context.temporary_folder

        return res

    def _validate_steps_context(self):
        assert self._rendering_context.flow_name

        flow_data = self._rendering_context.config_context["flows"][
            self._rendering_context.flow_name
        ]
        flow_description = flow_data["description"]

        self.logger.info("flow_description: '{}'".format(flow_description))

        self._rendering_context.previous_steps_values = list()

        rendering_environment = self._create_rendering_environment()
        secret_database_context = self._rendering_context.secrets_context["databases"][
            self._rendering_context.database_name
        ]

        assert self._steps_factory
        for step_context in flow_data["steps"]:
            step_name = step_context["name"]
            step_description = step_context.get("description", "<no description>")

            self.logger.debug(
                "validating step '{}': {}".format(step_name, step_description)
            )

            if not self._steps_factory.is_step_known(step_name):
                raise ConfigurationValidationException(
                    "Unknown step '{}'".format(step_name)
                )

            self.logger.debug("performing dry run for step '{}'".format(step_name))

            secret_context = dict()
            if ("steps_configuration" in secret_database_context) and (
                step_name in secret_database_context["steps_configuration"]
            ):
                secret_context = secret_database_context["steps_configuration"][
                    step_name
                ]

            step_object = self._steps_factory.create_object(
                step_name,
                logger=self.logger,
                rendering_context=self._rendering_context,
                step_context=step_context,
                secret_context=secret_context,
                rendering_environment=rendering_environment,
            )

            additional_variables = step_object.run(dry_run=True)
            self.logger.debug("additional_variables: {}".format(additional_variables))

            self._rendering_context.previous_steps_values.append(additional_variables)

    def _get_all_required_parameters_for_step(self, step_name, step_context):
        res = dict()
        exclude_root_elements = ["name", "description", "generates"]
        for step_context_item_name, step_context_item_value in step_context.items():
            if step_context_item_name in exclude_root_elements:
                continue

            res[step_context_item_name] = self._extract_all_parameters_low(
                step_name, step_context_item_name, step_context_item_value
            )

        self.logger.debug("parameters for step: {}".format(res))

        return res
        # flow_context = self._rendering_context.config_context["flows"][self._rendering_context.flow_name]

    def _extract_all_parameters_low(self, step_name, parameter_name, parameter_value):

        if isinstance(parameter_value, list) or isinstance(parameter_value, tuple):
            res = list()

            for item in parameter_value:
                res.extend(
                    self._extract_all_parameters_low(step_name, parameter_name, item)
                )

            return res

        elif isinstance(parameter_name, dict):
            res = list()

            for key, value in parameter_name.items():
                res.extend(self._extract_all_parameters_low(step_name, key, value))

            return res

        variables = jinja2schema.infer(parameter_value)
        return [value_name for value_name, _ in variables.items()]

    def _get_database_name(self, args):
        if args.database:
            return args.database

        assert self._rendering_context.secrets_context

        return self._rendering_context.secrets_context["defaults"]["database"]

    def _get_flow_name(self, args):
        if args.flow:
            return args.flow

        assert self._rendering_context.secrets_context
        assert self._rendering_context.database_name

        database_context = self._rendering_context.secrets_context["databases"][
            self._rendering_context.database_name
        ]
        return database_context["flow_type"]

    def _get_temporary_folder(self, args):
        if args.temporary_folder:
            return args.temporary_folder

        temporary_folder = self._rendering_context.secrets_context["defaults"][
            "temporary_folder"
        ]
        if temporary_folder:
            return temporary_folder

        return self._rendering_context.config_context["defaults"]["temporary_folder"]

    def _create_folder_name_for_execution(self):
        res = "{}_{}".format(
            self._backup_start_timestamp.isoformat(), str(uuid.uuid4())
        )
        res = res.replace(":", "")
        res = res.replace("-", "")
        return res

    @staticmethod
    def _create_rendering_environment():
        env = Environment(loader=BaseLoader, undefined=StrictUndefined)

        env.filters["extract_year_four_digits"] = jinja2_custom_filter_extract_year_four_digits
        env.filters["extract_month_two_digits"] = jinja2_custom_filter_extract_month_two_digits
        env.filters["extract_day_two_digits"] = jinja2_custom_filter_extract_day_two_digits

        return env


class YabtoolApplication(object):
    def __init__(self, logger):
        self.logger = logger
        self._rendering_context = None

    def run(self):
        args = get_cli_args()

        context_initializer = RenderingContextInitializer(self.logger)
        self._rendering_context = context_initializer.initialize(args)

        return True
