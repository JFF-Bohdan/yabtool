import codecs
import hashlib
import os
import re
import subprocess

import boto3

from .base import BaseFlowStep, DryRunExecutionError
from .s3boto_client import S3BacicBotoClient


class FileUploadingError(BaseException):
    pass


class ThirdPartyCommandsExecutor(object):
    @staticmethod
    def execute(command):
        return subprocess.run(command, stdout=subprocess.PIPE, stdin=subprocess.PIPE)


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
            self.logger.info("going to execute: {}".format(command))
            result = ThirdPartyCommandsExecutor.execute(command)
        else:
            self.logger.info("going to execute: {}".format(dry_run_command))
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

        # file_name_in_validation_file = self._render_parameter("file_name_in_validation_file")
        # self.step_context["file_name_in_validation_file"] = file_name_in_validation_file

        hash_type = self.step_context["hash_type"]
        algorithms_available = [str(item).lower() for item in hashlib.algorithms_available]
        self.logger.debug("algorithms_available: {}".format(algorithms_available))

        if hash_type not in algorithms_available:
            raise DryRunExecutionError("")

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
            self.logger.info("going to execute: {}".format(command))
            result = ThirdPartyCommandsExecutor.execute(command)
        else:
            self.logger.info("going to execute: {}".format(dry_run_command))
            result = ThirdPartyCommandsExecutor.execute(dry_run_command)

        self.logger.info("returncode: {}".format(result.returncode))
        self.logger.info("stderr: {}".format(result.stderr))
        self.logger.info("stdout: {}".format(result.stdout))

        if not dry_run:
            result.check_returncode()

        return super().run(dry_run)

    @classmethod
    def step_name(cls):
        return "7z_comress"


class Validate7ZArchive(BaseFlowStep):

    def run(self, dry_run=False):
        command = self._render_parameter("command_template")
        self.step_context["command"] = command

        dry_run_command = self._render_parameter("dry_run_command")
        self.step_context["dry_run_command"] = dry_run_command

        if not dry_run:
            self.logger.info("going to execute: {}".format(command))
            result = ThirdPartyCommandsExecutor.execute(command)
        else:
            self.logger.info("going to execute: {}".format(dry_run_command))
            result = ThirdPartyCommandsExecutor.execute(dry_run_command)

        self.logger.info("returncode: {}".format(result.returncode))
        self.logger.info("stderr: {}".format(result.stderr))
        self.logger.info("stdout: {}".format(result.stdout))

        if not dry_run:
            result.check_returncode()

        return super().run(dry_run)

    @classmethod
    def step_name(cls):
        return "validate_7z_arhive"


class S3FileUpload(BaseFlowStep):
    # TODO: support dry run
    def run(self, dry_run=False):
        S3_BUCKET_NAME_REGEX = "^[a-zA-Z0-9.\-_]{1,255}$"

        self.logger.debug("self.secret_context: {}".format(self.secret_context))

        bucket_name = self.secret_context["bucket_name"]
        self.logger.debug("bucket_name: '{}'".format(bucket_name))

        if dry_run:
            pattern = re.compile(S3_BUCKET_NAME_REGEX)
            if not pattern.match(bucket_name):
                raise DryRunExecutionError("base bucket name should match regex '{}'".format(S3_BUCKET_NAME_REGEX))

        region = self.secret_context["region"]

        raw_client = self._crete_s3_client()

        client = S3BacicBotoClient(self.logger, raw_client)

        if not dry_run:
            if not client.bucket_exists(bucket_name):
                self.logger.info("creating bucker '{}'".format(bucket_name))
                client.create_bucket(bucket_name, region=region)

        database_prefix_in_bucket = self._render_parameter("database_prefix_in_bucket")
        self.logger.debug("database_prefix_in_bucket: '{}'".format(database_prefix_in_bucket))

        targets = self.mixed_context["source_files"]

        real_source_file_names = self._get_real_source_file_names(targets)
        self.logger.info("going to upload these files:\n\t{}".format("\n\t".format(real_source_file_names)))

        additional_context = {"database_prefix_in_bucket": database_prefix_in_bucket}

        rules = self.mixed_context["upload_rules"]
        for rule in rules:
            self.logger.info("processing upload rule '{}'".format(rule["name"]))
            self._upload_for_rule(
                dry_run,
                client,
                bucket_name,
                rule,
                real_source_file_names,
                additional_context
            )

        return super().run(dry_run)

    def _crete_s3_client(self):
        region = self.secret_context.get("region")
        self.logger.debug("S3 region: '{}'".format(region))

        return boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=self.secret_context["aws_access_key_id"],
            aws_secret_access_key=self.secret_context["aws_secret_access_key"]
        )

    @classmethod
    def step_name(cls):
        return "s3_multipart_upload"

    def _get_real_source_file_names(self, targets):
        res = list()

        for target in targets:
            source_file_name = target["source_file"]

            source_file_name = self._render_result(source_file_name)
            self.logger.debug("source_file: {}".format(source_file_name))
            if not os.path.exists(source_file_name):
                self.logger.error("Can't find source file '{}'".format(source_file_name))
                raise FileUploadingError("Can't find source file: '{}'".format(source_file_name))

            res.append(source_file_name)

        return res

    def _upload_for_rule(
            self,
            dry_run,
            basic_client,
            bucket_name,
            rule,
            files_on_local_drive,
            additional_context=None
    ):
        destination_prefix = self._render_result(rule["destination_prefix"], additional_context)
        self.logger.debug("destination_prefix: '{}'".format(destination_prefix))

        validation_name = self._render_result(rule["validation_name"], additional_context)
        validation_name = os.path.join(destination_prefix, validation_name)
        validation_name = str(validation_name).replace("\\", "/")
        self.logger.debug("validation_name: '{}'".format(validation_name))

        if basic_client.is_object_exists(bucket_name, validation_name):
            self.logger.info("file '{}' already exists in remote bucket, SKIPPING".format(validation_name))
            return

        if dry_run:
            return

        basic_client.create_marker_object(bucket_name, validation_name)

        for local_file in files_on_local_drive:
            assert os.path.exists(local_file)

            dest_object_name = os.path.join(destination_prefix, os.path.basename(local_file))
            dest_object_name = str(dest_object_name).replace("\\", "/")

            self.logger.info("dest_object_name: '{}'".format(dest_object_name))

            basic_client.put_object(
                bucket_name,
                dest_object_name,
                local_file
            )
