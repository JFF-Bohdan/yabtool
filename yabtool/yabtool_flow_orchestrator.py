import codecs
import copy
import datetime
import os
import uuid

import terminaltables
from yabtool.shared.jinja2_helpers import create_rendering_environment
from yaml import safe_load

from .supported_steps import create_steps_factory
from .supported_steps.base import pretty_time_delta, time_interval
from .yabtool_stat import StepExecutionStatisticEntry

DEFAULT_CONFIG_RELATIVE_NAME = "./config/config.yaml"


class ConfigurationValidationException(BaseException):
    pass


class RenderingContext(object):
    def __init__(self):
        self.config_file_name = None
        self.secrets_file_name = None

        self.config_context = dict()
        self.secrets_context = dict()
        self.target_name = None
        self.flow_name = None

        self.basic_values = dict()
        self.previous_steps_values = list()
        self.temporary_folder = None
        self.root_temporary_folder = None

        self.remove_temporary_folder = None
        self.perform_dry_run = None
        self.unknown_args = None

    def to_context(self):
        res = self.basic_values
        for item in self.previous_steps_values:
            res = {**res, **item}

        return res


class YabtoolFlowOrchestrator(object):
    def __init__(self, logger):
        self.rendering_context = RenderingContext()
        self.logger = logger
        self._steps_factory = None
        self._backup_start_timestamp = datetime.datetime.utcnow()
        self._skip_flow_execution_voting_result = None
        self.skip_voting_enabled = True

        self.dry_run_statistics = []
        self.active_run_statistics = []

    def initialize(self, args, unknown_args):
        self.rendering_context.config_file_name = self._get_config_file_name(args)
        self.logger.debug(
            "config_file_name: '{}'".format(self.rendering_context.config_file_name)
        )

        if not self.rendering_context.config_file_name:
            raise ConfigurationValidationException("No configuration file specified")

        if not os.path.exists(self.rendering_context.config_file_name):
            raise ConfigurationValidationException(
                "Configuration file does not exists. Path: '{}'".format(
                    self.rendering_context.config_file_name
                )
            )

        self.rendering_context.config_context = self._load_yaml_file(
            self.rendering_context.config_file_name
        )

        self.rendering_context.unknown_args = unknown_args

        self.rendering_context.secrets_file_name = self._get_secrets_file_name(args)
        if not self.rendering_context.secrets_file_name:
            raise ConfigurationValidationException(
                "Secrets file is not specified"
            )

        if not os.path.exists(self.rendering_context.secrets_file_name):
            raise ConfigurationValidationException(
                "Secrets file does not exists. Path: '{}'".format(
                    self.rendering_context.secrets_file_name
                )
            )

        self.logger.debug("loading secrets from: '{}'".format(self.rendering_context.secrets_file_name))
        self.rendering_context.secrets_context = self._load_yaml_file(
            self.rendering_context.secrets_file_name
        )

        self.rendering_context.target_name = self._get_target_name(args)
        self.logger.debug("target_name: '{}'".format(self.target_name))

        self.rendering_context.flow_name = self._get_flow_name(args)
        self.logger.debug("flow_name: '{}'".format(self.rendering_context.flow_name))

        self._override_config_parameters_with_secrets()

        self.rendering_context.remove_temporary_folder = self.config_context["parameters"]["remove_temporary_folder"]
        self.rendering_context.perform_dry_run = self.config_context["parameters"]["perform_dry_run"] or args.dry_run

        self.rendering_context.root_temporary_folder = self._get_temporary_folder(args)

        self.rendering_context.temporary_folder = os.path.join(
            self.rendering_context.root_temporary_folder,
            self._create_folder_name_for_execution(),
        )

        os.makedirs(self.rendering_context.temporary_folder)

        self.logger.debug(
            "temporary_folder: '{}'".format(self.rendering_context.temporary_folder)
        )

        self._steps_factory = create_steps_factory()

        self.rendering_context.basic_values = self._init_basic_values()
        self.logger.debug(
            "basic_values: {}".format(self.rendering_context.basic_values)
        )

        return True

    def dry_run(self):
        self.logger.warning("performing dry run")
        if self.rendering_context.perform_dry_run:
            self._run(dry_run=True)

    def run(self):
        self.logger.warning("performing active run")
        self._run(dry_run=False)

    def print_stat(self):
        if self.dry_run_statistics:
            stat_data = self.produce_exeuction_stat(self.dry_run_statistics)
            self.logger.info("{}:\n{}".format("Dry run statistics:", stat_data))
            metrics_data_list = self.produce_execution_metrics(self.dry_run_statistics)
            for step_name, metrics_data_item in metrics_data_list:
                self.logger.info("Metrics for '{}':\n{}".format(step_name, metrics_data_item))

        if self.active_run_statistics:
            stat_data = self.produce_exeuction_stat(self.active_run_statistics)
            self.logger.info("{}:\n{}".format("Execution statistics:", stat_data))
            metrics_data_list = self.produce_execution_metrics(self.active_run_statistics)
            for step_name, metrics_data_item in metrics_data_list:
                self.logger.info("Metrics for '{}':\n{}".format(step_name, metrics_data_item))

        if (not self.dry_run_statistics) and (not self.active_run_statistics):
            self.logger.info("No execution statistics")

    def produce_exeuction_stat(self, stat_source):
        header = ["Step Name", "Exexcution start timestamp", "Execution end timestamp", "Time elapsed "]
        data = [header]

        total_time_elapsed_seconds = 0
        max_length = len(data[0]) if data else 0
        print_format = "%Y-%m-%d %H:%M:%S"
        for statistics_item in stat_source:
            # step_name = "{} ({})".format(statistics_item.step_human_readable_name, statistics_item.step_name)
            step_name = statistics_item.step_name
            time_elapsed_in_seconds = time_interval(
                statistics_item.execution_start_timestamp,
                statistics_item.execution_end_timestamp,
            )

            data_row = [
                step_name,
                statistics_item.execution_start_timestamp.strftime(print_format),
                statistics_item.execution_end_timestamp.strftime(print_format),
                pretty_time_delta(time_elapsed_in_seconds)
            ]

            data.append(data_row)
            max_length = max_length if max_length <= len(data_row) else len(data_row)

            total_time_elapsed_seconds += time_elapsed_in_seconds

        data_row = [""] * (max_length - 2)
        data_row.append("Total")
        data_row.append(pretty_time_delta(total_time_elapsed_seconds))
        data.append(data_row)

        table = terminaltables.AsciiTable(data)
        return table.table

    def produce_execution_metrics(self, stat_source):
        res = []
        for statistics_item in stat_source:
            step_name = "{} ({})".format(statistics_item.step_human_readable_name, statistics_item.step_name)

            metrics = statistics_item.metrics
            if metrics.is_empty():
                continue

            metrics_data = [["Name", "Value"]]

            for metric_name in metrics.get_all_metrics():
                metric = metrics.get_metric(metric_name)
                metrics_data.append(
                    [
                        metric.metric_name,
                        "{} {}".format(
                            metric.value,
                            metric.units_name
                        )
                    ]
                )

            table = terminaltables.AsciiTable(metrics_data)
            res_item = (step_name, table.table)
            res.append(res_item)

        return res

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

            if not os.path.exists(config_file_name):
                self.logger.error("can't find configuration file in '{}'".format(config_file_name))

            assert os.path.exists(config_file_name), config_file_name
        else:
            config_file_name = os.path.abspath(config_file_name)
            config_file_name = os.path.normpath(config_file_name)

        return config_file_name

    def _run(self, dry_run):
        assert self.rendering_context.flow_name

        if dry_run:
            self._skip_flow_execution_voting_result = None

        flow_data = self.rendering_context.config_context["flows"][self.flow_name]
        flow_description = flow_data["description"]

        self.logger.info("flow_description: '{}'".format(flow_description))

        self.rendering_context.previous_steps_values = []

        rendering_environment = create_rendering_environment()
        secret_targets_context = self.rendering_context.secrets_context["targets"][self.target_name]

        self._execute_steps(dry_run, flow_data, rendering_environment, secret_targets_context)

    def _execute_steps(self, dry_run, flow_data, rendering_environment, secret_targets_context):
        assert self._steps_factory

        statistics_list = self.dry_run_statistics if dry_run else self.active_run_statistics
        statistics_list.clear()

        if self._need_skip_voting():
            self.logger.warning("Want skip flow execution")
            return

        positive_votes_for_flow_execution_skipping = []
        for step_context in flow_data["steps"]:
            step_name = step_context["name"]
            step_human_readable_name = step_context.get("human_readable_name", step_name)

            step_description = step_context.get("description", "<no description>")

            self.logger.debug(
                "validating step '{}': {}".format(step_name, step_description)
            )

            if not self._steps_factory.is_step_known(step_name):
                raise ConfigurationValidationException(
                    "Unknown step '{}'".format(step_name)
                )

            if dry_run:
                self.logger.debug("performing dry run for step '{}'".format(step_name))
            else:
                self.logger.debug("performing active run for step '{}'".format(step_name))

            secret_context = dict()
            relative_secrets = step_context.get("relative_secrets", [])
            required_secrets = [step_name]
            required_secrets.extend(relative_secrets)

            for required_secret in required_secrets:
                if (
                    ("steps_configuration" in secret_targets_context) and  # noqa
                    (required_secret in secret_targets_context["steps_configuration"])
                ):
                    secret_context = {
                        **secret_context,
                        **secret_targets_context["steps_configuration"][required_secret]
                    }

            step_object = self._steps_factory.create_object(
                step_name,
                logger=self.logger,
                rendering_context=self.rendering_context,
                step_context=step_context,
                secret_context=secret_context,
                rendering_environment=rendering_environment,
            )

            if dry_run:
                self.logger.info("initializing dry run for step: '{}'".format(step_name))
                self.logger.debug("checking for decision for flow skipping")
                self._check_for_flow_execution_skipping(step_object, positive_votes_for_flow_execution_skipping)
            else:
                self.logger.info("initializing active run for step: '{}'".format(step_name))

            stat_entry = StepExecutionStatisticEntry(
                step_name=step_name,
                step_human_readable_name=step_human_readable_name,
                execution_start_timestamp=datetime.datetime.utcnow()
            )
            additional_variables = step_object.run(stat_entry, dry_run=dry_run)
            stat_entry.execution_end_timestamp = datetime.datetime.utcnow()

            statistics_list.append(stat_entry)

            self.logger.debug("additional_variables: {}".format(additional_variables))

            self.rendering_context.previous_steps_values.append(additional_variables)

        if dry_run and positive_votes_for_flow_execution_skipping:
            self.logger.info(
                "Flow execution can be SKIPPED.\n\tThese steps voted to skip flow execution: {}".format(
                    positive_votes_for_flow_execution_skipping
                )
            )
            self._skip_flow_execution_voting_result = True

    def _check_for_flow_execution_skipping(self, step_object, positive_votes_for_flow_execution_skipping):
        step_name = step_object.step_name()

        if self._skip_flow_execution_voting_result is not None:
            self.logger.debug(
                "decision for flow execution skipping already made. Can skip flow execution: {}".format(
                    self._skip_flow_execution_voting_result
                )
            )
            return

        vote = step_object.vote_for_flow_execution_skipping()
        if vote is None:
            self.logger.debug("step '{}' do not want to vote for flow execution skipping".format(step_name))
            return

        self.logger.debug("step '{}' voted for flow execution skipping".format(step_name))

        if not vote:
            self._skip_flow_execution_voting_result = False
            self.logger.debug("step '{}' voted against flow execution skipping".format(step_name))
        else:
            positive_votes_for_flow_execution_skipping.append(step_name)
            self.logger.debug("step '{}' voted for flow execution skipping".format(step_name))

    def _get_target_name(self, args):
        if args.target:
            return args.target

        assert self.rendering_context.secrets_context

        return self.rendering_context.secrets_context["defaults"]["target"]

    def _get_flow_name(self, args):
        if args.flow:
            return args.flow

        assert self.rendering_context.secrets_context
        assert self.rendering_context.target_name

        targets_context = self.rendering_context.secrets_context["targets"][
            self.rendering_context.target_name
        ]
        return targets_context["flow_type"]

    def _override_config_parameters_with_secrets(self):
        if "parameters" in self.secrets_context:
            override_params = self.secrets_context["parameters"]
            for key, value in override_params.items():
                self.config_context["parameters"][key] = value

        secret_targets_context = self.secrets_context["targets"][self.target_name]
        if "config_patch" in secret_targets_context:
            config_patch = secret_targets_context["config_patch"]

            patched_steps_count = 0
            for step_patch in config_patch["steps"]:
                step_patch_data = copy.deepcopy(step_patch)
                self.logger.debug("step_patch_data: {}".format(step_patch_data))
                del step_patch_data["name"]

                patched_steps_count += self._patch_step_in_flow(step_patch["name"], step_patch_data)

            if patched_steps_count:
                flow_data = self.rendering_context.config_context["flows"][self.flow_name]
                self.logger.debug("flow_data:\n{}".format(flow_data))

    def _patch_step_in_flow(self, step_name, step_patch_data):
        flow_data = self.rendering_context.config_context["flows"][self.flow_name]
        flow_steps = flow_data["steps"]

        patched_steps_count = 0
        flow_steps_count = len(flow_steps)
        for index in range(flow_steps_count):
            flow_step = flow_steps[index]
            if flow_step["name"] != step_name:
                continue

            self.logger.warning(
                "patching step '{}' for flow '{}' with:\n{}".format(
                    step_name,
                    self.flow_name,
                    step_patch_data
                )
            )
            flow_steps[index] = {**flow_step, **step_patch_data}
            patched_steps_count += 1

        return patched_steps_count

    def _get_temporary_folder(self, args):
        if args.temporary_folder:
            return args.temporary_folder

        temporary_folder = self.rendering_context.secrets_context["defaults"]["temporary_folder"]
        if temporary_folder:
            return temporary_folder

        return self.rendering_context.config_context["defaults"]["temporary_folder"]

    def _need_skip_voting(self):
        if not self.skip_voting_enabled:
            return False

        if (self._skip_flow_execution_voting_result is not None) and self._skip_flow_execution_voting_result:
            return True

        return False

    def _create_folder_name_for_execution(self):
        res = "{}_{}".format(self._backup_start_timestamp.isoformat(), str(uuid.uuid4()))
        res = res.replace(":", "")
        res = res.replace("-", "")
        return res

    @staticmethod
    def _load_yaml_file(file_name, codec="utf-8"):
        with codecs.open(file_name, "r", codec) as input_file:
            return safe_load(input_file.read())

    @staticmethod
    def _get_secrets_file_name(args):
        secrets_file_name = args.secrets
        if not secrets_file_name:
            return None

        secrets_file_name = os.path.abspath(secrets_file_name)
        secrets_file_name = os.path.normpath(secrets_file_name)

        return secrets_file_name

    def _init_basic_values(self):
        res = self._get_additional_rendering_variables()

        res["main_target_name"] = self.rendering_context.target_name
        res["week_day_short_name"] = self._backup_start_timestamp.strftime("%a")
        res["week_number"] = self._backup_start_timestamp.strftime("%U")
        res["month_short_name"] = self._backup_start_timestamp.strftime("%b")
        res["month_two_digit_number"] = self._backup_start_timestamp.strftime("%m")
        res["backup_start_timestamp"] = self._backup_start_timestamp
        res["flow_name"] = self.flow_name
        res["yabtool_exec_folder"] = self.rendering_context.temporary_folder

        res["current_year"] = self._backup_start_timestamp.strftime("%Y")
        res["current_month"] = res["month_two_digit_number"]
        res["current_day_of_month"] = self._backup_start_timestamp.strftime("%d")

        res["current_date"] = self._backup_start_timestamp.strftime("%Y-%m-%d")
        res["current_time"] = self._backup_start_timestamp.strftime("%H%M%S")

        res["lower"] = str.lower
        res["upper"] = str.upper

        return res

    def _get_additional_rendering_variables(self):
        targets_context = self.rendering_context.secrets_context["targets"][self.target_name]

        res = targets_context.get("additional_variables", {})
        self.logger.info("additional variables: {}".format(res))

        return res

    @property
    def config_context(self):
        return self.rendering_context.config_context

    @property
    def secrets_context(self):
        return self.rendering_context.secrets_context

    @property
    def flow_name(self):
        return self.rendering_context.flow_name

    @property
    def target_name(self):
        return self.rendering_context.target_name

    @property
    def backup_start_timestamp(self):
        return self._backup_start_timestamp
