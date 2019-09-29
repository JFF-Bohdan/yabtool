from botocore.exceptions import ClientError


class S3BotoClient(object):
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

    def bucket_exists(self, bucket_name):
        try:
            _ = self._client.head_bucket(Bucket=bucket_name)  # noqa
        except ClientError as e:
            self.logger.debug(e)
            return False

        return True

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

        # Construct Body= parameter
        if isinstance(src_data, bytes):
            object_data = src_data
        elif isinstance(src_data, str):
            try:
                object_data = open(src_data, "rb")
                # possible FileNotFoundError/IOError exception
            except Exception as e:
                self.logger.error(e)
                return False
        else:
            self.logger.error("Type of {} for the argument 'src_data' is not supported.".format(str(type(src_data))))
            return False

        # Put the object
        try:
            self._client.put_object(Bucket=dest_bucket_name, Key=dest_object_name, Body=object_data)
        except ClientError as e:
            # AllAccessDisabled error == bucket not found
            # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
            self.logger.error(e)
            return False

        finally:
            if isinstance(src_data, str):
                object_data.close()

        return True
