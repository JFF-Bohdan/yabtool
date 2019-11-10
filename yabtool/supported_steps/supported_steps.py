import codecs
import hashlib
import os
import subprocess

from .base import BaseFlowStep, DryRunExecutionError


class FileUploadingError(BaseException):
    pass


class ThirdPartyCommandsExecutor(object):
    @staticmethod
    def execute(command):
        result = subprocess.run(command, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

        result.stdout = result.stdout if result.stdout is not None else bytes()
        result.stderr = result.stderr if result.stderr is not None else bytes()

        return result


class MakeDirectoryForBackup(BaseFlowStep):
    def run(self, dry_run=False):

        res = self._render_parameter("generation_mask")
        res = os.path.normpath(os.path.abspath(res))

        if not dry_run:
            os.makedirs(res)

        self.additional_output_context = {"result": res}

        return super().run(dry_run)

    @classmethod
    def step_name(cls):
        return "mkdir_for_backup"


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

        if not dry_run:
            self.logger.info("Making backup of Firebird database")
            self.logger.debug("going to execute: {}".format(command))
            result = ThirdPartyCommandsExecutor.execute(command)
        else:
            self.logger.debug("going to execute: {}".format(dry_run_command))
            result = ThirdPartyCommandsExecutor.execute(dry_run_command)

        if not dry_run:
            result.check_returncode()

        return super().run(dry_run)

    @classmethod
    def step_name(cls):
        return "firebird_backup"


class CalculateFileHashAndSaveToFile(BaseFlowStep):
    def run(self, dry_run=False):
        input_file_name = self._render_parameter("input_file_name")
        self.step_context["input_file_name"] = input_file_name

        output_file_name = self._render_parameter("output_file_name")
        self.step_context["output_file_name"] = output_file_name

        hash_type = self.step_context["hash_type"]
        algorithms_available = [str(item).lower() for item in hashlib.algorithms_available]
        self.logger.debug("algorithms_available: {}".format(algorithms_available))

        if hash_type not in algorithms_available:
            raise DryRunExecutionError("unsupported hash type '{}'".format(hash_type))

        if not dry_run:
            self.logger.info("going calculate hash ('{}') for '{}'".format(hash_type, input_file_name))
            hash_value = self._hash_file(input_file_name, hash_type)
            output_data = "{} *{}\n".format(hash_value, os.path.basename(input_file_name))
            self._save_data(output_file_name, output_data)

        return super().run(dry_run)

    @staticmethod
    def _save_data(file_name, data, codepage="utf-8"):
        with codecs.open(file_name, "w", codepage) as output_file:
            output_file.write(data)

    @staticmethod
    def _hash_file(file_name, hash_type):
        BLOCKSIZE = 65536

        hasher = hashlib.new(hash_type)
        with open(file_name, "rb") as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)

        return str(hasher.hexdigest())

    @classmethod
    def step_name(cls):
        return "calculate_file_hash_and_save_in_file"


class CompressFileWithSevenZ(BaseFlowStep):

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


class Validate7ZArchive(BaseFlowStep):

    def run(self, dry_run=False):
        command = self._render_parameter("command_template")
        self.step_context["command"] = command

        dry_run_command = self._render_parameter("dry_run_command")
        self.step_context["dry_run_command"] = dry_run_command

        if not dry_run:
            self.logger.info("Validating 7Z archive")
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
        return "validate_7z_archive"
