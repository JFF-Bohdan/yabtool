import os
import re

import boto3

from .base import BaseFlowStep, DryRunExecutionError, TransmissionError
from .s3boto_client import S3BacicBotoClient


class S3FileUpload(BaseFlowStep):
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

        database_prefix_in_bucket = self._render_parameter("database_prefix_in_bucket")
        self.logger.debug("database_prefix_in_bucket: '{}'".format(database_prefix_in_bucket))

        targets = self.mixed_context["source_files"]

        additional_context = {"database_prefix_in_bucket": database_prefix_in_bucket}
        upload_rules = self.mixed_context["upload_rules"]

        if dry_run:
            self.logger.debug("checking that bucket exists to perform dry run")
            client.bucket_exists(bucket_name)
            return super().run(dry_run)

        if not client.bucket_exists(bucket_name):
            self.logger.info("creating bucker '{}'".format(bucket_name))
            client.create_bucket(bucket_name, region=region)

        real_source_file_names = self._get_real_source_file_names(targets)
        self.logger.info("going to upload these files:\n\t{}".format("\n\t".format(real_source_file_names)))

        for rule in upload_rules:
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

    def _get_real_source_file_names(self, targets):
        res = list()

        for target in targets:
            source_file_name = target["source_file"]

            source_file_name = self._render_result(source_file_name)
            self.logger.debug("source_file: {}".format(source_file_name))
            if not os.path.exists(source_file_name):
                msg = "Can't find source file '{}'".format(source_file_name)
                self.logger.error(msg)
                raise TransmissionError(msg)

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

            basic_client.upload_file(
                bucket_name,
                dest_object_name,
                local_file
            )

    @classmethod
    def step_name(cls):
        return "s3_multipart_upload"
