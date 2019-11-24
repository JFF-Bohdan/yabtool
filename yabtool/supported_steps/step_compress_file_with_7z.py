from .base import BaseFlowStep
from .shared import ThirdPartyCommandsExecutor


class StepCompressFileWith7Z(BaseFlowStep):

    def run(self, dry_run=False):
        output_archive_name = self._render_parameter("output_archive_name")
        self.step_context["output_archive_name"] = output_archive_name

        command = self._render_parameter("command_template")
        self.step_context["command"] = command

        dry_run_command = self._render_parameter("dry_run_command")
        self.step_context["dry_run_command"] = dry_run_command

        if not dry_run:
            self.logger.info("Compressing file with 7Z archive")
            self.logger.debug("going to execute: {}".format(command))
            result = ThirdPartyCommandsExecutor.execute(command)
        else:
            self.logger.debug("going to execute: {}".format(dry_run_command))
            result = ThirdPartyCommandsExecutor.execute(dry_run_command)

        self.logger.info("returncode: {}".format(result.returncode))

        if not dry_run:
            self.logger.info("stderr:\n{}".format(result.stderr.decode("utf-8")))
            self.logger.info("stdout:\n{}".format(result.stdout.decode("utf-8")))

        if not dry_run:
            result.check_returncode()

        return super().run(dry_run)

    @classmethod
    def step_name(cls):
        return "7z_compress"
