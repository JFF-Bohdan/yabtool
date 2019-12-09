import os
import re

from .base import DryRunExecutionError, time_interval
from .s3_steps_shared import StepS3FileBaseUploader, UploadTarget
from .s3boto_client import S3BasicBotoClient


class StepS3MultipartUploadWithRotation(StepS3FileBaseUploader):
    def __init__(self, **kwargs):
        self._first_uploads_key_name_per_files = {}
        super().__init__(**kwargs)

    def vote_for_flow_execution_skipping(self):
        self.logger.debug("checking if need skip flow execution for step '{}'".format(self.step_name))

        bucket_name = self.secret_context["bucket_name"]
        self.logger.debug("bucket_name: '{}'".format(bucket_name))

        raw_client = self._crete_s3_client()
        client = S3BasicBotoClient(self.logger, raw_client)

        prefix_in_bucket = self._render_parameter("prefix_in_bucket")
        self.logger.debug("prefix_in_bucket: '{}'".format(prefix_in_bucket))
        self.step_context["prefix_in_bucket"] = prefix_in_bucket

        target_prefix_in_bucket = self._render_parameter("target_prefix_in_bucket")
        self.logger.debug("target_prefix_in_bucket: '{}'".format(target_prefix_in_bucket))

        additional_context = {"target_prefix_in_bucket": target_prefix_in_bucket}
        upload_rules = self.mixed_context["upload_rules"]

        if not client.is_bucket_exists(bucket_name):
            self.logger.debug("Step can't be skipped, because bucket is not exists")
            return False

        for rule in upload_rules:
            self.logger.info("processing upload rule '{}'".format(rule["name"]))

            if not self._can_skip_execution_for_rule(
                client,
                bucket_name,
                rule,
                additional_context
            ):
                return False

        return True

    def run(self, stat_entry, dry_run=False):
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

        targets = self._get_upload_targets()

        additional_context = {"target_prefix_in_bucket": target_prefix_in_bucket}
        upload_rules = self.mixed_context["upload_rules"]

        if dry_run:
            self.logger.debug("checking that bucket exists to perform dry run")
            client.is_bucket_exists(bucket_name)
            return super().run(dry_run)

        if not client.is_bucket_exists(bucket_name):
            self.logger.info("creating bucker '{}'".format(bucket_name))
            client.create_bucket(bucket_name, region=region)

        targets = self._get_real_source_file_names_for_targets(targets)

        self.logger.info("going to upload these files:\n\t{}".format(targets))

        for rule in upload_rules:
            self.logger.info("processing upload rule '{}'".format(rule["name"]))
            self._upload_for_rule(
                stat_entry,
                client,
                bucket_name,
                rule,
                targets,
                additional_context
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

    def _get_upload_targets(self):
        raw_data = self.mixed_context["source_files"]

        res = []
        for raw_item in raw_data:
            res_item = UploadTarget()
            res_item.source_file = raw_item["source_file"]
            res_item.add_dedup_tag = raw_item.get("add_dedup_tag", False)

            res.append(res_item)

        return res

    def _can_skip_execution_for_rule(
        self,
        basic_client,
        bucket_name,
        rule,
        additional_context=None
    ):
        destination_prefix = self._render_result(rule["destination_prefix"], additional_context)
        self.logger.debug("destination_prefix: '{}'".format(destination_prefix))

        dedup_tag_name = self._render_result(rule["dedup_tag_name"], additional_context)
        dedup_tag_value = self._render_result(rule.get("dedup_tag_value"), additional_context)

        self.logger.debug(
            "looking for objects with tag '{}' and tag value '{}'".format(
                dedup_tag_name,
                dedup_tag_value
            )
        )
        tagged_key = self._get_tagged_object_key(
            basic_client,
            bucket_name,
            destination_prefix,
            dedup_tag_name,
            dedup_tag_value
        )
        self.logger.debug("tagged_key = '{}'".format(tagged_key))

        if tagged_key:
            self.logger.info("tag for iteration already exists: '{}'".format(tagged_key))
            return True

        return False

    def _upload_for_rule(
        self,
        stat_entry,
        basic_client,
        bucket_name,
        rule,
        upload_targets,
        additional_context=None
    ):
        destination_prefix = self._render_result(rule["destination_prefix"], additional_context)
        self.logger.debug("destination_prefix: '{}'".format(destination_prefix))

        # TODO: rename in configuration file
        dedup_tag_name = self._render_result(rule["dedup_tag_name"], additional_context)
        dedup_tag_value = self._render_result(rule.get("dedup_tag_value"), additional_context)
        marking_tags = {
            dedup_tag_name: dedup_tag_value
        }

        self.logger.debug(
            "looking for objects with tag '{}' and tag value '{}'".format(
                dedup_tag_name,
                dedup_tag_value
            )
        )
        tagged_key = self._get_tagged_object_key(
            basic_client,
            bucket_name,
            destination_prefix,
            dedup_tag_name,
            dedup_tag_value
        )
        self.logger.debug("tagged_key = '{}'".format(tagged_key))

        if tagged_key:
            self.logger.info("tag for iteration already exists: '{}'".format(tagged_key))
            return

        existing_files_for_rule = self._load_already_existing_files_for_rule(
            basic_client,
            bucket_name,
            destination_prefix
        )

        for upload_target in upload_targets:
            assert os.path.exists(upload_target.os_file_name)

            dest_key_name = os.path.join(destination_prefix, os.path.basename(upload_target.os_file_name))
            dest_key_name = str(dest_key_name).replace("\\", "/")

            self.logger.info("dest_key_name: '{}'".format(dest_key_name))

            first_upload_key_name = self._first_uploads_key_name_per_files.get(upload_target.os_file_name, None)
            self.logger.debug(
                "first_upload_key_name: '{}' for file '{}'".format(
                    first_upload_key_name,
                    upload_target.os_file_name
                )
            )

            if not first_upload_key_name:
                self.logger.info("no previous uploads available - FRESH UPLOAD")
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

                self._first_uploads_key_name_per_files[upload_target.os_file_name] = dest_key_name
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

                metric = self._get_metric_by_name(
                    stat_entry,
                    StepS3FileBaseUploader.METRIC_COPIED_OBJECTS_COUNT,
                    initial_value=0,
                    units_name="items"
                )
                metric.increment(1)

            if dest_key_name in existing_files_for_rule:
                existing_files_for_rule.remove(dest_key_name)

            if upload_target.add_dedup_tag:
                basic_client.set_object_tags(bucket_name, dest_key_name, marking_tags)

        self._remove_files_existing_for_rule(stat_entry, basic_client, bucket_name, existing_files_for_rule)

    def _load_already_existing_files_for_rule(self, basic_client, bucket_name, destination_prefix):
        self.logger.debug("checking for files that already exists in bucket")
        existing_files_for_rule = basic_client.list_files_in_folder(bucket_name, destination_prefix)
        self.logger.debug("existing_files_for_rule: {}".format(existing_files_for_rule))

        return existing_files_for_rule

    def _remove_files_existing_for_rule(self, stat_entry, basic_client, bucket_name, existing_files_for_rule):
        existing_files_base_names = [os.path.basename(item) for item in existing_files_for_rule]
        self.logger.info("some files already exists in folder for rule: {}".format(existing_files_base_names))

        for existing_file in existing_files_for_rule:
            self.logger.info("removing item '{}'".format(existing_file))
            basic_client.delete_object(bucket_name, existing_file)

            metric = self._get_metric_by_name(
                stat_entry,
                StepS3FileBaseUploader.METRIC_DELETED_OBJECTS_COUNT,
                initial_value=0,
                units_name="items"
            )
            metric.increment(1)

    @classmethod
    def step_name(cls):
        return "s3_multipart_upload_with_rotation"
