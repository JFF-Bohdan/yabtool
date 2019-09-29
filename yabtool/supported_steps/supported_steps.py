import codecs
import hashlib
import os
import re
import subprocess

import boto3

from .base import BaseFlowStep, DryRunExecutionError
from .s3boto_client import S3BotoClient


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


class S3MultipartFileUpload(BaseFlowStep):
    def run(self, dry_run=False):
        S3_BUCKET_NAME_REGEX = "^[a-zA-Z0-9.\-_]{1,255}$"
        DEST_NAME_TEMPLATE = "{{base_prefix}}{{backup_type}}/{{source_file|base_name}}"

        self.logger.debug("self.secret_context: {}".format(self.secret_context))

        base_bucket_prefix = self.secret_context["base_bucket_prefix"]

        if dry_run:
            pattern = re.compile(S3_BUCKET_NAME_REGEX)
            if not pattern.match(base_bucket_prefix):
                raise DryRunExecutionError("base bucket name should match regex '{}'".format(S3_BUCKET_NAME_REGEX))

        region = self.secret_context["region"]

        targets = self.step_context["source_files"]
        raw_client = self._crete_s3_client()

        client = S3BotoClient(self.logger, raw_client)

        if not dry_run:
            if not client.bucket_exists(base_bucket_prefix):
                self.logger.info("creating bucker '{}'".format(base_bucket_prefix))
                client.create_bucket(base_bucket_prefix, region=region)

        backup_type = "flat"

        prefix = self._render_parameter("prefix")
        s3_name_generation_context = {
            "base_prefix": prefix,
            "backup_type": backup_type
        }

        for target_file in targets:
            source_file = target_file["source_file"]

            source_file = self._render_result(source_file)
            self.logger.debug("source_file: {}".format(source_file))

            if not dry_run:
                assert os.path.exists(source_file)

            s3_name_generation_context["source_file"] = source_file
            dest_file_name = self._render_result(DEST_NAME_TEMPLATE, s3_name_generation_context)

            self.logger.debug(
                "going to upload with:\n\tbase_bucket_prefix: '{}'\n\tdest_file_name: '{}'\n\tsource_file: '{}'".format(
                    base_bucket_prefix,
                    dest_file_name,
                    source_file
                )
            )

            if not dry_run:
                client.put_object(base_bucket_prefix, dest_file_name, source_file)

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
