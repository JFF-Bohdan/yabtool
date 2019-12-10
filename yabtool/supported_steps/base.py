import datetime
import os


class DryRunExecutionError(Exception):
    pass


class TransmissionError(Exception):
    pass


class WrongParameterTypeError(Exception):
    pass


class StepContextData(object):
    def __init__(self):
        self.name = None
        self.description = None


def pretty_time_delta(seconds):
    sign_string = "-" if seconds < 0 else ""
    seconds = abs(seconds)

    days, seconds = divmod(seconds, 86400)
    days = int(days)

    hours, seconds = divmod(seconds, 3600)
    hours = int(hours)

    minutes, seconds = divmod(seconds, 60)
    minutes = int(minutes)

    seconds = round(seconds, 3)

    if days > 0:
        return "{}{}d{}h{}m{}s".format(sign_string, days, hours, minutes, seconds)
    elif hours > 0:
        return "{}{}h{}m{}s".format(sign_string, hours, minutes, seconds)
    elif minutes > 0:
        return "{}{}m{}s".format(sign_string, minutes, seconds)
    else:
        return "{}{:.3f}s".format(sign_string, seconds)


def time_interval(timestamp_start, timestamp_end):
    time_elapsed = (timestamp_end - timestamp_start)
    res = float(time_elapsed.total_seconds())

    res += round(time_elapsed.microseconds / 1000000, 3)
    return res


class BaseFlowStep(object):
    BYTES_IN_MEGABYTE = 1024 * 1024

    def __init__(
        self,
        logger,
        rendering_context,
        step_context,
        secret_context,
        rendering_environment,
    ):
        self.logger = logger
        self.rendering_context = rendering_context
        self.step_context = step_context
        self.rendering_environment = rendering_environment
        self.secret_context = secret_context
        self.additional_output_context = None

    @classmethod
    def step_name(cls):
        pass

    @property
    def mixed_context(self):
        return self._get_mixed_context()

    def run(self, stat_entry, dry_run=False):
        output_variables = self._generate_output_variables()
        self.logger.debug("output_variables: {}".format(output_variables))

        return output_variables

    def vote_for_flow_execution_skipping(self):
        return None

    def _render_parameter(self, parameter_name, context=None):
        if not context:
            context = self.mixed_context

        template = context[parameter_name]
        self.logger.debug("'{}'@template: '{}'".format(parameter_name, template))

        res = self._render_result(template)
        self.logger.debug("'{}'@value: '{}'".format(parameter_name, res))

        return res

    def _get_mixed_context(self):
        mixed_context = self.rendering_context.to_context()
        mixed_context = {**mixed_context, **self._get_step_context()}

        return mixed_context

    def _get_step_context(self):
        return {**self.secret_context, **self.step_context}

    def _render_result(self, template, additional_context=None):
        if not template:
            return ""

        mixed_context = self.mixed_context

        if additional_context:
            mixed_context = {**mixed_context, **additional_context}

        return self._render_from_template_and_context(template, mixed_context)

    def _render_from_template_and_context(self, template, context):
        jinja2_template = self.rendering_environment.from_string(template)
        return jinja2_template.render(**context)

    def _generate_output_variables(self):
        res = dict()

        if "generates" not in self.step_context:
            return res

        for requested_value_name, requested_value_template in self.step_context["generates"].items():
            res[requested_value_name] = self._render_result(requested_value_template, self.additional_output_context)

        return res

    def _get_metric_by_name(self, stat_entry, metric_name, initial_value=None, units_name=None):
        return stat_entry.metrics.get_metric(metric_name, initial_value=initial_value, units_name=units_name)

    def _get_file_size_in_mibs(self, file_name):
        file_size = os.path.getsize(file_name)

        size_in_megs = (file_size / BaseFlowStep.BYTES_IN_MEGABYTE)
        return size_in_megs

    def _get_current_timestamp(self):
        return datetime.datetime.utcnow()
