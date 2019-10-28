import os
import re

import boto3

from .base import BaseFlowStep, DryRunExecutionError, TransmissionError
from .s3boto_client import S3BacicBotoClient


class S3FileUpload(BaseFlowStep):
    def __init__(self, **kwargs):
        self._first_uploads_key_name_per_files = {}
        self._already_existing_files_for_rule = []
        super().__init__(**kwargs)

    def run(self, dry_run=False):
        S3_BUCKET_NAME_REGEX = r"^[a-zA-Z0-9.\-_]{1,255}$"

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
            client.is_bucket_exists(bucket_name)
            return super().run(dry_run)

        if not client.is_bucket_exists(bucket_name):
            self.logger.info("creating bucker '{}'".format(bucket_name))
            client.create_bucket(bucket_name, region=region)

        real_source_file_names = self._get_real_source_file_names(targets)

        self.logger.info("going to upload these files:\n\t{}".format(real_source_file_names))

        for rule in upload_rules:
            self.logger.info("processing upload rule '{}'".format(rule["name"]))
            self._upload_for_rule(
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

    def _upload_for_rule(
        self,
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

        self._already_existing_files_for_rule = self._load_already_existing_files_for_rule(
            basic_client,
            bucket_name,
            destination_prefix
        )

        for local_file in files_on_local_drive:
            assert os.path.exists(local_file)

            dest_key_name = os.path.join(destination_prefix, os.path.basename(local_file))
            dest_key_name = str(dest_key_name).replace("\\", "/")

            self.logger.info("dest_key_name: '{}'".format(dest_key_name))

            first_upload_key_name = self._first_uploads_key_name_per_files.get(local_file, None)
            self.logger.debug("first_upload_key_name: '{}' for file '{}'".format(first_upload_key_name, local_file))

            if not first_upload_key_name:
                self.logger.info("no previous uploads available - FRESH UPLOAD")
                self.logger.info("bucket_name: '{}', dest_key_name: '{}'".format(bucket_name, dest_key_name))

                basic_client.upload_file(
                    bucket_name,
                    dest_key_name,
                    local_file
                )
                self._first_uploads_key_name_per_files[local_file] = dest_key_name
            else:
                self.logger.info(
                    "previous upload available in key '{}' will COPY to key '{}'".format(
                        first_upload_key_name,
                        dest_key_name
                    )
                )

                basic_client.copy_file_from_one_bucket_to_another(
                    bucket_name,
                    first_upload_key_name,
                    bucket_name,
                    dest_key_name
                )

            if dest_key_name in self._already_existing_files_for_rule:
                self._already_existing_files_for_rule.remove(dest_key_name)

        self._remove_files_existing_for_rule(basic_client, bucket_name, destination_prefix)
        basic_client.create_marker_object(bucket_name, validation_name)

    def _load_already_existing_files_for_rule(self, basic_client, bucket_name, destination_prefix):
        self.logger.debug("checking for files that already exists in bucket")
        existing_files_for_rule = basic_client.list_files_in_folder(bucket_name, destination_prefix)
        self.logger.debug("existing_files_for_rule: {}".format(existing_files_for_rule))

        return existing_files_for_rule

    def _remove_files_existing_for_rule(self, basic_client, bucket_name, destination_prefix):
        existing_files_base_names = [os.path.basename(item) for item in self._already_existing_files_for_rule]
        self.logger.info("some files already exists in folder for rule: {}".format(existing_files_base_names))

        for existing_file in self._already_existing_files_for_rule:
            self.logger.info("removing item '{}'".format(existing_file))
            basic_client.delete_object(bucket_name, existing_file)

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

    @classmethod
    def step_name(cls):
        return "s3_multipart_upload"
