import os
import subprocess

from .base import BaseFlowStep


class ThirdPartyCommandsExecutor(object):
    def execute(self, command):
        return subprocess.run(command, stdout=subprocess.PIPE, stdin=subprocess.PIPE)


class MakeDirectoryForBackup(BaseFlowStep):
    @classmethod
    def step_name(cls):
        return "mkdir_for_backup"

    def run(self, dry_run=False):

        res = self._render_parameter("generation_mask")
        res = os.path.normpath(os.path.abspath(res))

        if not dry_run:
            os.makedirs(res)

        return self._generate_output_variables(res)

    def _generate_output_variables(self, step_result):
        additional_context = {"result": step_result}

        res = dict()
        for requested_value_name, requested_value_template in self.step_context[
            "generates"
        ].items():
            res[requested_value_name] = self._render_result(
                requested_value_template, additional_context
            )

        return res


class MakeFirebirdDatabaseBackup(BaseFlowStep):
    def run(self, dry_run=False):
        backup_log_name = self._render_parameter("backup_log_name")
        self.step_context["backup_log_name"] = backup_log_name

        backup_file_name = self._render_parameter("backup_file_name")
        self.step_context["backup_file_name"] = backup_file_name

        command = self._render_parameter("command_template")
        self.step_context["command"] = command

        dry_run_command = self._render_parameter("dry_run_command")
        self.step_context["dry_run_command"] = dry_run_command

        executor = ThirdPartyCommandsExecutor()
        if not dry_run:
            self.logger.info("going to execute: {}".format(command))
            result = executor.execute(command)
        else:
            self.logger.info("going to execute: {}".format(dry_run_command))
            result = executor.execute(dry_run_command)

        if not dry_run:
            result.check_returncode()

        return super().run(dry_run)

    @classmethod
    def step_name(cls):
        return "firebird_backup"


class CalculateFileHashAndSaveToFile(BaseFlowStep):
    @classmethod
    def step_name(cls):
        return "calculate_file_hash_and_save_in_file"


class CompressFileWithSevenZ(BaseFlowStep):
    @classmethod
    def step_name(cls):
        return "7z_comress"


class Validate7ZArchive(BaseFlowStep):
    @classmethod
    def step_name(cls):
        return "validate_7z_arhive"


class S3MultipartFileUpload(BaseFlowStep):
    @classmethod
    def step_name(cls):
        return "s3_multipart_upload"
