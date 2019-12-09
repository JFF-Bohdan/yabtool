import copy
import os

import boto3
from yabtool.shared.base import AttrsToStringMixin

from .base import BaseFlowStep, TransmissionError


class UploadTarget(AttrsToStringMixin):
    def __init__(self):
        self.source_file = None
        self.add_dedup_tag = False
        self.os_file_name = None


class StepS3FileBaseUploader(BaseFlowStep):
    S3_BUCKET_NAME_REGEX = r"^[a-zA-Z0-9.\-_]{1,255}$"

    METRIC_UPLOADED_OBJECTS_COUNT = "Uploaded Objects"
    METRIC_UPLOADED_SIZE = "Uploaded Size"
    METRIC_TRANSMISSION_TIME = "Transmission Time"
    METRIC_TRANSMISSION_SPEED = "Transmission Speed"
    METRIC_COPIED_OBJECTS_COUNT = "Copied Objects Count"
    METRIC_DELETED_OBJECTS_COUNT = "Deleted Objects Count"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._client = None

    def _crete_s3_client(self):
        region = self.secret_context.get("region")
        self.logger.debug("S3 region: '{}'".format(region))

        return boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=self.secret_context["aws_access_key_id"],
            aws_secret_access_key=self.secret_context["aws_secret_access_key"]
        )

    def _get_tagged_object_key(
        self,
        basic_client,
        bucket_name,
        destination_prefix,
        validation_tag_name,
        validation_tag_value
    ):
        objects_for_prefix = basic_client.list_files_in_folder(bucket_name, destination_prefix)

        for object_key in objects_for_prefix:
            object_tags = basic_client.get_object_tags(bucket_name, object_key)

            self.logger.debug("tags for key '{}': {}".format(object_key, object_tags))

            if validation_tag_name not in object_tags:
                continue

            if object_tags[validation_tag_name] == validation_tag_value:
                return object_key

        return None

    def _get_real_source_file_names_for_targets(self, targets):
        res = []

        for target in targets:
            new_target = copy.deepcopy(target)

            source_file_name = new_target.source_file
            source_file_name = self._render_result(source_file_name)
            self.logger.debug("source_file: {}".format(source_file_name))
            if not os.path.exists(source_file_name):
                msg = "Can't find source file '{}'".format(source_file_name)
                self.logger.error(msg)
                raise TransmissionError(msg)

            new_target.os_file_name = source_file_name
            res.append(new_target)

        return res
