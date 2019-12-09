import argparse
import os
import re

from .base import DryRunExecutionError, time_interval
from .s3_steps_shared import StepS3FileBaseUploader, UploadTarget
from .s3boto_client import S3BasicBotoClient


class StepS3StrictUploader(StepS3FileBaseUploader):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._execution_suffix = None

    def run(self, stat_entry, dry_run=False):
        self._execution_suffix = self._get_upload_suffix()
        self.logger.debug("execution_suffix: {}".format(self._execution_suffix))
        self.step_context["execution_suffix"] = self._execution_suffix

        bucket_name = self.secret_context["bucket_name"]
        self.logger.debug("bucket_name: '{}'".format(bucket_name))

        if dry_run:
            pattern = re.compile(StepS3FileBaseUploader.S3_BUCKET_NAME_REGEX)
            if not pattern.match(bucket_name):
                raise DryRunExecutionError(
                    "base bucket name should match regex '{}'".format(StepS3FileBaseUploader.S3_BUCKET_NAME_REGEX)
                )

        region = self.secret_context["region"]

        raw_client = self._crete_s3_client()
        client = S3BasicBotoClient(self.logger, raw_client)

        prefix_in_bucket = self._render_parameter("prefix_in_bucket")
        self.logger.debug("prefix_in_bucket: '{}'".format(prefix_in_bucket))
        self.step_context["prefix_in_bucket"] = prefix_in_bucket

        target_prefix_in_bucket = self._render_parameter("target_prefix_in_bucket")
        self.logger.debug("target_prefix_in_bucket: '{}'".format(target_prefix_in_bucket))

        uploads = self._get_uploads()
        self.logger.debug("uploads: {}".format(uploads))

        # additional_context = {"target_prefix_in_bucket": target_prefix_in_bucket}
        # uploads = self.mixed_context["uploads"]

        if dry_run:
            self.logger.debug("checking that bucket exists to perform dry run")
            client.is_bucket_exists(bucket_name)
            return super().run(dry_run)

        if not client.is_bucket_exists(bucket_name):
            self.logger.info("creating bucker '{}'".format(bucket_name))
            client.create_bucket(bucket_name, region=region)

        uploads = self._get_real_source_file_names_for_targets(uploads)
        self.logger.info("going to upload these files:\n\t{}".format(uploads))

        for upload_target in uploads:
            self.logger.info("processing upload rule '{}'".format(upload_target))
            self._upload_file(
                stat_entry,
                client,
                bucket_name,
                target_prefix_in_bucket,
                upload_target
            )

        uploaded_size_metric = self._get_metric_by_name(
            stat_entry,
            StepS3FileBaseUploader.METRIC_UPLOADED_SIZE
        )

        upload_time_metric = self._get_metric_by_name(
            stat_entry,
            StepS3FileBaseUploader.METRIC_TRANSMISSION_TIME
        )

        if uploaded_size_metric.value and upload_time_metric.value:
            transmission_speed_metric = self._get_metric_by_name(
                stat_entry,
                StepS3FileBaseUploader.METRIC_TRANSMISSION_SPEED,
                units_name="MiB/s"
            )

            if upload_time_metric.value:
                transmission_speed_metric.value = round(uploaded_size_metric.value / upload_time_metric.value, 2)
            else:
                transmission_speed_metric.value = "N/A"

        if uploaded_size_metric.value:
            uploaded_size_metric.value = round(uploaded_size_metric.value, 2)

        if upload_time_metric.value:
            upload_time_metric.value = round(upload_time_metric.value, 3)

        return super().run(dry_run)

    def _get_uploads(self):
        raw_data = self.mixed_context["uploads"]

        res = []
        for raw_item in raw_data:
            res_item = UploadTarget()
            res_item.source_file = raw_item["source_file"]
            res.append(res_item)

        return res

    def _upload_file(
        self,
        stat_entry,
        basic_client,
        bucket_name,
        target_prefix_in_bucket,
        upload_target
    ):
        assert os.path.exists(upload_target.os_file_name)

        dest_key_name = os.path.join(target_prefix_in_bucket, os.path.basename(upload_target.os_file_name))
        dest_key_name = str(dest_key_name).replace("\\", "/")

        self.logger.info("bucket_name: '{}', dest_key_name: '{}'".format(bucket_name, dest_key_name))

        transmission_start_timestamp = self._get_current_timestamp()
        basic_client.upload_file(
            bucket_name,
            dest_key_name,
            upload_target.os_file_name
        )
        transmission_end_timestamp = self._get_current_timestamp()

        metric = self._get_metric_by_name(
            stat_entry,
            StepS3FileBaseUploader.METRIC_UPLOADED_OBJECTS_COUNT,
            initial_value=0,
            units_name="items"
        )
        metric.increment(1)

        metric = self._get_metric_by_name(
            stat_entry,
            StepS3FileBaseUploader.METRIC_UPLOADED_SIZE,
            initial_value=0.0,
            units_name="MiB"
        )
        size_in_mibs = self._get_file_size_in_mibs(upload_target.os_file_name)
        metric.increment(size_in_mibs)

        metric = self._get_metric_by_name(
            stat_entry,
            StepS3FileBaseUploader.METRIC_TRANSMISSION_TIME,
            initial_value=0.0,
            units_name="seconds"
        )
        metric.increment(time_interval(transmission_start_timestamp, transmission_end_timestamp))

    def _get_upload_suffix(self):
        unknown_args = self.rendering_context.unknown_args
        self.logger.debug("unknown_args: {}".format(unknown_args))

        parser = self._init_arg_parser()
        args, _ = parser.parse_known_args(unknown_args)
        upload_suffix = None
        if args.upload_suffix:
            upload_suffix = str(args.upload_suffix).strip()
            upload_suffix = None if not upload_suffix else upload_suffix

        if upload_suffix and (upload_suffix[0] != "-"):
            upload_suffix = "-" + upload_suffix

        return upload_suffix

    @staticmethod
    def _init_arg_parser():
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--upload-suffix",
            action="store",
            help="Upload suffix for strict upload to S3"
        )

        return parser

    @classmethod
    def step_name(cls):
        return "step_s3_strict_upload"
