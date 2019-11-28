from .base import BaseFlowStep, time_interval
from .shared import ThirdPartyCommandsExecutor


class StepValidate7ZArchive(BaseFlowStep):

    def run(self, stat_entry, dry_run=False):
        output_archive_name = self._render_parameter("output_archive_name")
        self.step_context["output_archive_name"] = output_archive_name

        command = self._render_parameter("command_template")
        self.step_context["command"] = command

        dry_run_command = self._render_parameter("dry_run_command")
        self.step_context["dry_run_command"] = dry_run_command

        timestamp_execution_start = self._get_current_timestamp()
        if not dry_run:
            self.logger.info("Validating 7Z archive")
            self.logger.debug("going to execute: {}".format(command))
            result = ThirdPartyCommandsExecutor.execute(command)
        else:
            self.logger.debug("going to execute: {}".format(dry_run_command))
            result = ThirdPartyCommandsExecutor.execute(dry_run_command)

        self.logger.info("return code: {}".format(result.returncode))
        timestamp_execution_end = self._get_current_timestamp()

        if not dry_run:
            self.logger.info("stderr:\n{}".format(result.stderr.decode("utf-8")))
            self.logger.info("stdout:\n{}".format(result.stdout.decode("utf-8")))

        if not dry_run:
            result.check_returncode()

            size_in_mibs = self._get_file_size_in_mibs(output_archive_name)
            spen_time = time_interval(timestamp_execution_start, timestamp_execution_end)
            speed_in_mibs = (size_in_mibs / spen_time) if spen_time else "N/A"

            self._get_metric_by_name(
                stat_entry,
                "Validated Size",
                initial_value=round(size_in_mibs, 2),
                units_name="MiB"
            )

            self._get_metric_by_name(
                stat_entry,
                "Validation Speed",
                initial_value=round(speed_in_mibs, 2),
                units_name="MiB/s"
            )

        return super().run(dry_run)

    @classmethod
    def step_name(cls):
        return "validate_7z_archive"
