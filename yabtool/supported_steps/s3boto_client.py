import os
import threading

from boto3.s3.transfer import MB, S3Transfer, TransferConfig
from botocore.exceptions import ClientError

from .base import WrongParameterTypeError


class ProgressPercentage(object):
    def __init__(self, logger, filename):
        self._filename = filename
        self._bytes_to_transmit = float(os.path.getsize(filename))
        self._transmitted_bytes_count = 0
        self._lock = threading.Lock()
        self._logger = logger

    def __call__(self, bytes_amount):
        with self._lock:
            self._transmitted_bytes_count += bytes_amount
            percentage = (self._transmitted_bytes_count / self._bytes_to_transmit) * 100
            percentage = round(percentage, 2)
            self._logger.info("transmitted: {}%".format(percentage))

    @staticmethod
    def _get_file_size(file_name):
        return os.path.getsize(file_name)


class S3BasicBotoClient(object):
    DEFAULT_TRANSMISSION_CHUNK_SIZE = 8 * MB
    DEFAULT_NOTIFICATION_THRESHHOLD = 1 * MB
    DEFAULT_TRANSMISSION_MAX_THREADS = 20
    DEFAULT_MAX_TRANSMISSION_ATTEMPTS = 5

    def __init__(self, logger, s3_client):
        self.logger = logger
        self._client = s3_client

    def create_bucket(self, bucket_name, region=None):
        try:
            if region is None:
                self._client.create_bucket(Bucket=bucket_name)
            else:
                location = {"LocationConstraint": region}
                self._client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration=location
                )

        except ClientError as e:
            self.logger.error(e)
            return False

        return True

    def is_object_exists(self, bucket_name, object_name):
        try:
            self._client.head_object(Bucket=bucket_name, Key=object_name)
        except ClientError:
            return False

        return True

    def is_bucket_exists(self, bucket_name):
        try:
            _ = self._client.head_bucket(Bucket=bucket_name)  # noqa
        except ClientError as e:
            self.logger.debug(e)
            return False

        return True

    def upload_file(
        self,
        dest_bucket_name,
        dest_object_name,
        source_file_name,
        transfer_config=None
    ):
        if transfer_config is None:
            transfer_config = TransferConfig(
                multipart_threshold=S3BasicBotoClient.DEFAULT_NOTIFICATION_THRESHHOLD,
                max_concurrency=S3BasicBotoClient.DEFAULT_TRANSMISSION_MAX_THREADS,
                multipart_chunksize=S3BasicBotoClient.DEFAULT_TRANSMISSION_CHUNK_SIZE,
                num_download_attempts=S3BasicBotoClient.DEFAULT_MAX_TRANSMISSION_ATTEMPTS,
                use_threads=True
            )

        transfer = S3Transfer(self._client, config=transfer_config)

        transfer.upload_file(
            source_file_name,
            dest_bucket_name,
            dest_object_name,
            callback=ProgressPercentage(self.logger, source_file_name),
        )

        return True

    def copy_file_from_one_bucket_to_another(
        self,
        src_bucket_name,
        src_object_name,
        dest_bucket_name,
        dest_object_name,
    ):
        copy_source = {
            "Bucket": src_bucket_name,
            "Key": src_object_name
        }
        self._client.copy(copy_source, dest_bucket_name, dest_object_name)

    def put_object(self, dest_bucket_name, dest_object_name, src_data):
        """Add an object to an Amazon S3 bucket

        The src_data argument must be of type bytes or a string that references
        a file specification.

        :param dest_bucket_name: string
        :param dest_object_name: string
        :param src_data: bytes of data or string reference to file spec
        :return: True if src_data was added to dest_bucket/dest_object, otherwise
        False
        """

        object_data = None
        need_close = False
        try:
            if isinstance(src_data, bytes):
                object_data = src_data
            elif isinstance(src_data, str):
                need_close = True
                object_data = open(src_data, "rb")
            else:
                msg = "Type of {} for the argument 'src_data' is not supported.".format(str(type(src_data)))
                self.logger.error(msg)
                raise WrongParameterTypeError(msg)

            self._put_object(dest_bucket_name, dest_object_name, object_data)

        finally:
            if need_close:
                object_data.close()

    def list_files_in_folder(self, bucket_name, folder=""):
        response = self._client.list_objects(Bucket=bucket_name, Prefix=folder)
        return [content.get("Key") for content in response.get("Contents", [])]

    def delete_object(self, bucket_name, key):
        self._client.delete_object(Bucket=bucket_name, Key=key)

    def get_object_tags(self, bucket_name, key):
        ret = {}

        resp = self._client.get_object_tagging(Bucket=bucket_name, Key=key)
        if "TagSet" not in resp:
            return ret

        tags_set = resp["TagSet"]
        for tags_set_item in tags_set:
            ret[tags_set_item["Key"]] = tags_set_item["Value"]

        return ret

    def set_object_tags(self, bucket_name, key, tags):
        tags_list = [{"Key": str(key), "Value": str(value)} for key, value in tags.items()]
        self._client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging={"TagSet": tags_list})

    def delete_object_tags(self, bucket_name, key):
        self._client.get_object_tagging(Bucket=bucket_name, Key=key)

    def _put_object(self, dest_bucket_name, dest_object_name, object_data):
        # Put the object
        try:
            self._client.put_object(Bucket=dest_bucket_name, Key=dest_object_name, Body=object_data)
        except Exception as e:
            # AllAccessDisabled error == bucket not found
            # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
            self.logger.error(e)
            raise
