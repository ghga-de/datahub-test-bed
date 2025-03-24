# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Storage client and related models."""

import hashlib
import logging
import os
import re
import tempfile

import botocore.exceptions

from datahub_test_bed.validations.exceptions import (
    UnexpectedHTTPStatusException,
)
from datahub_test_bed.validations.models import BaseBotoClient
from datahub_test_bed.validations.utils import (
    PART_COUNT,
    PART_SIZE,
    TEST_FILE_PREFIX,
    generate_testfile,
    get_error_message,
    log_error,
)

logger = logging.getLogger("storage")

# Allow colon character in bucket names (for Ceph multi-tenancy, etc.)
botocore.handlers.VALID_BUCKET = re.compile(
    r"^(?:[a-zA-Z0-9_]{1,191}:)?[a-z0-9\-]{3,63}$"
)


class StorageClient(BaseBotoClient):
    """A client for interacting with S3/Ceph storage."""

    def head_bucket(self, bucket: str, expect_error: bool = False):
        """Check if the bucket is accessible."""
        expected_status = 200
        try:
            response = self.s3_client.head_bucket(Bucket=bucket)
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == expected_status:
                logger.info(
                    'Bucket "%s" is accessible for account "%s"',
                    bucket,
                    self.profile_name,
                )
                if expect_error:
                    logger.error(
                        'Bucket "%s" should not be accessible for account "%s"',
                        bucket,
                        self.profile_name,
                    )
                return
            error_message = (
                f"Unexpected HTTP status code {status}, expected {expected_status}"
            )
            raise UnexpectedHTTPStatusException(error_message)
        except (
            botocore.exceptions.ClientError,
            UnexpectedHTTPStatusException,
        ) as error:
            error_message = get_error_message(error)
            log_error(
                operation="HeadBucket",
                account_name=self.profile_name,
                bucket=bucket,
                error_message=error_message,
                expect_error=expect_error,
            )

    def head_object(self, bucket: str, key: str) -> int:
        """Check if the object exists and return its size."""
        expected_status = 200
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == expected_status:
                logger.info(
                    'Object in bucket "%s" is accessible for account "%s"',
                    bucket,
                    self.profile_name,
                )
                return response.get("ContentLength", 0)
            error_message = (
                f"Unexpected HTTP status code {status}, expected {expected_status}"
            )
            raise UnexpectedHTTPStatusException(error_message)
        except (
            botocore.exceptions.ClientError,
            UnexpectedHTTPStatusException,
        ) as error:
            error_message = get_error_message(error)
            log_error(
                operation="HeadObject",
                account_name=self.profile_name,
                bucket=bucket,
                key=key,
                error_message=error_message,
            )
            return -1

    def list_all_object_in_bucket(
        self,
        *,
        bucket: str,
        prefix: str | None = None,
        return_objects: bool = False,
        expect_error: bool = False,
    ):
        """List objects in a bucket. If prefix is provided, look for specific keys."""
        expected_status = 200
        try:
            if prefix:
                resp = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            else:
                resp = self.s3_client.list_objects_v2(Bucket=bucket)
            status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == expected_status:
                if not return_objects:
                    logger.info(
                        'Objects in bucket "%s" listed successfully for account "%s"',
                        bucket,
                        self.profile_name,
                    )
                if expect_error:
                    logger.error(
                        'Account "%s" should not be able to list objects in bucket "%s"',
                        self.profile_name,
                        bucket,
                    )
                if return_objects:
                    if "Contents" in resp:
                        return resp["Contents"]
                    else:
                        logger.info("No objects found")
                        return []
                return
            error_message = (
                f"Unexpected HTTP status code {status}, expected {expected_status}"
            )
            raise UnexpectedHTTPStatusException(error_message)
        except (
            botocore.exceptions.ClientError,
            UnexpectedHTTPStatusException,
        ) as error:
            error_message = get_error_message(error)
            log_error(
                operation="ListObjectV2",
                account_name=self.profile_name,
                bucket=bucket,
                error_message=error_message,
                expect_error=expect_error,
            )

    def create_multipart_upload(
        self, bucket: str, key: str, expect_error=False
    ) -> str | None:
        """Create a multipart upload and return the upload ID."""
        try:
            resp = self.s3_client.create_multipart_upload(Bucket=bucket, Key=key)
            return resp.get("UploadId")
        except botocore.exceptions.ClientError as error:
            error_message = get_error_message(error)
            log_error(
                operation="CreateMultipartUpload",
                account_name=self.profile_name,
                bucket=bucket,
                error_message=error_message,
                expect_error=expect_error,
            )
            return None

    def upload_part(
        self, *, bucket: str, key: str, part_num: int, part_path: str, upload_id: str
    ) -> str | None:
        """Upload a part of a file to S3."""
        if not os.path.isfile(part_path):
            logger.error(
                "Part file %s does not exist.",
                part_path,
            )
            return None
        with open(part_path, "rb") as f:
            part_sha256 = hashlib.sha256(f.read()).hexdigest()
        try:
            with open(part_path, "rb") as data:
                logger.debug("Uploading part %s ...", part_num)
                resp = self.s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_num,
                    Body=data,
                    UploadId=upload_id,
                    ChecksumSHA256=part_sha256,
                )
            return resp.get("ETag")
        except botocore.exceptions.ClientError as error:
            error_message = get_error_message(error)
            log_error(
                operation="UploadPart",
                account_name=self.profile_name,
                bucket=bucket,
                error_message=error_message,
            )
            return None

    def upload_part_copy(  # noqa: PLR0913
        self,
        *,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
        part_num: int,
        upload_id: str,
        copy_source_range: str,
    ) -> str | None:
        """Copy a part of an object using multipart copy."""
        try:
            logger.debug("Copying part %s ...", part_num)
            resp = self.s3_client.upload_part_copy(
                Bucket=dst_bucket,
                Key=dst_key,
                PartNumber=part_num,
                UploadId=upload_id,
                CopySource={"Bucket": src_bucket, "Key": src_key},
                CopySourceRange=copy_source_range,
            )
            return resp.get("CopyPartResult", {}).get("ETag")
        except botocore.exceptions.ClientError as error:
            error_message = get_error_message(error)
            log_error(
                operation="UploadPartCopy",
                account_name=self.profile_name,
                bucket=dst_bucket,
                error_message=error_message,
            )
            return None

    def split_file_and_upload_parts(
        self, *, bucket: str, file_path: str, key: str, upload_id: str
    ) -> list[dict[str, object]]:
        """Split the file into parts and upload to S3 using NamedTemporaryFile."""
        etags = []
        with open(file_path, "rb") as source:
            for part_num in range(1, PART_COUNT + 1):
                chunk = source.read(PART_SIZE)
                if not chunk:
                    break
                with tempfile.NamedTemporaryFile("w+b") as temp_file:
                    temp_file.write(chunk)
                    etag = self.upload_part(
                        bucket=bucket,
                        key=key,
                        part_num=part_num,
                        part_path=temp_file.name,
                        upload_id=upload_id,
                    )
                    if not etag:
                        logger.debug(
                            'Failed to upload part %s to "%s" for "%s" using "%s"',
                            part_num,
                            bucket,
                            key,
                            self.profile_name,
                        )
                        return []
                    etags.append({"ETag": etag, "PartNumber": part_num})
        return etags

    def complete_multipart_upload(
        self, *, bucket: str, key: str, upload_id: str, etags: list
    ) -> bool:
        """Complete a multipart upload with the given parts."""
        try:
            resp = self.s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": etags},
            )
            return bool(resp.get("Location"))
        except botocore.exceptions.ClientError as error:
            error_message = get_error_message(error)
            log_error(
                operation="CompleteMultipartUpload",
                account_name=self.profile_name,
                bucket=bucket,
                error_message=error_message,
            )
            return False

    def upload_file_multipart(
        self, *, file_path: str, key: str, bucket: str, expect_error=False
    ) -> None:
        """Upload a file to S3 using multipart upload with temporary parts."""
        logger.info(f'Uploading {key} using account "{self.profile_name}"')
        upload_id = self.create_multipart_upload(bucket, key, expect_error=expect_error)

        if not upload_id:
            if not expect_error:
                logger.error("Failed to upload, upload ID not found")
            return

        if expect_error:
            logger.error(
                'Account "%s" should not be able to upload to bucket "%s"',
                self.profile_name,
                bucket,
            )
            return

        logger.info(
            'Multipart upload initiated for bucket "%s" using account "%s"',
            bucket,
            self.profile_name,
        )
        part_etags = self.split_file_and_upload_parts(
            bucket=bucket, file_path=file_path, key=key, upload_id=upload_id
        )
        if len(part_etags) != PART_COUNT:
            if expect_error:
                logger.info("Expected error: failed to upload all file parts")
            else:
                logger.debug(
                    'Failed to upload all file parts, expected "%s", got "%s"',
                    PART_COUNT,
                    len(part_etags),
                )
            return
        if expect_error:
            logger.error(
                'Account "%s" should not be able to upload file parts for bucket "%s"',
                self.profile_name,
                bucket,
            )
            return
        logger.debug(
            'Uploaded %i parts to bucket "%s" using account "%s"',
            len(part_etags),
            bucket,
            self.profile_name,
        )
        if self.complete_multipart_upload(
            bucket=bucket, key=key, upload_id=upload_id, etags=part_etags
        ):
            logger.info(
                'Multipart upload completed for bucket "%s" using account "%s"',
                bucket,
                self.profile_name,
            )

    def split_object_and_copy_parts(  # noqa: PLR0913
        self,
        size: int,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
        upload_id: str,
    ) -> list[dict[str, object]]:
        """Split an object into parts and copy them to another bucket."""
        etags = []
        start = 0
        part_num = 1
        while start < size:
            end = min(start + PART_SIZE - 1, size - 1)
            etag = self.upload_part_copy(
                src_bucket=src_bucket,
                src_key=src_key,
                dst_bucket=dst_bucket,
                dst_key=dst_key,
                part_num=part_num,
                upload_id=upload_id,
                copy_source_range=f"bytes={start}-{end}",
            )
            if not etag:
                logger.debug(
                    'Failed to copy part for object "%s" from bucket "%s" to bucket "%s" using account "%s"',
                    dst_key,
                    src_bucket,
                    dst_bucket,
                    self.profile_name,
                )
                return []
            etags.append({"ETag": etag, "PartNumber": part_num})
            start = end + 1
            part_num += 1
        return etags

    def copy_file_multipart(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
        expect_error: bool = False,
    ) -> None:
        """Copy an object from one bucket to another using multipart copy."""
        size = self.head_object(src_bucket, src_key)
        if not size or size < 0:
            logger.error(
                'Failed to retrieve object size from bucket "%s" for object "%s"',
                src_bucket,
                src_key,
            )
            return
        upload_id = self.create_multipart_upload(dst_bucket, dst_key)
        if not upload_id:
            if not expect_error:
                logger.error("Failed to upload, upload ID not found")
            return
        if expect_error:
            logger.error(
                'Account "%s" should not be able to upload to bucket "%s"',
                self.profile_name,
                dst_bucket,
            )
            return
        logger.info(
            'Multipart copy started from bucket "%s" to bucket "%s" using account "%s"',
            src_bucket,
            dst_bucket,
            self.profile_name,
        )

        etags = self.split_object_and_copy_parts(
            size, src_bucket, src_key, dst_bucket, dst_key, upload_id
        )
        if len(etags) != PART_COUNT:
            if expect_error:
                logger.info("Expected error: failed to copy all file parts")
            else:
                logger.debug(
                    'Failed to copy all file parts, expected "%i", got "%i"',
                    PART_COUNT,
                    len(etags),
                )
            return
        if expect_error:
            logger.error(
                'Account "%s" should not be able to copy file parts for bucket "%s"',
                self.profile_name,
                dst_bucket,
            )
            return
        logger.debug(
            'Copied %i parts to bucket "%s" using account "%s"',
            len(etags),
            dst_bucket,
            self.profile_name,
        )
        if self.complete_multipart_upload(
            bucket=dst_bucket, key=dst_key, upload_id=upload_id, etags=etags
        ):
            logger.info(
                'Multipart copy completed from bucket "%s" to bucket "%s" using account "%s"',
                src_bucket,
                dst_bucket,
                self.profile_name,
            )

    def get_presigned_url_for_object(
        self, bucket: str, key: str, expiration: int = 60
    ) -> str | None:
        """Generate a presigned URL for an object."""
        size = self.head_object(bucket, key)
        if not size or size < 0:
            return None
        try:
            url = self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expiration,
            )
            if "Signature" in url and "Expires" in url:
                logger.info(
                    'Successfully retrieved the presigned URL for object "%s" in bucket "%s" using account "%s"',
                    key,
                    bucket,
                    self.profile_name,
                )
                return url
            logger.warning(
                'Missing arguments in presigned URL for object "%s" in bucket "%s" using account "%s", URL: %s',
                key,
                bucket,
                self.profile_name,
                url,
            )
            return None

        except botocore.exceptions.ClientError as error:
            error_message = get_error_message(error)
            log_error(
                operation="GetPresignedUrl",
                account_name=self.profile_name,
                bucket=bucket,
                error_message=error_message,
            )
            return None

    def delete_object(self, bucket: str, key: str, expect_error: bool = False):
        """Delete an object from a bucket."""
        try:
            resp = self.s3_client.delete_object(Bucket=bucket, Key=key)
            if resp.get("ResponseMetadata", {}).get("HTTPStatusCode") == 204:
                logger.info(
                    'Successfully deleted object "%s" from bucket "%s" using account "%s"',
                    key,
                    bucket,
                    self.profile_name,
                )
                return True
            else:
                logger.error(
                    'Failed to delete object "%s" from bucket "%s" using account "%s"',
                    key,
                    bucket,
                    self.profile_name,
                )
            return False
        except botocore.exceptions.ClientError as error:
            error_message = get_error_message(error)
            log_error(
                operation="DeleteObject",
                account_name=self.profile_name,
                bucket=bucket,
                error_message=error_message,
                expect_error=expect_error,
            )

    def upload_test_file(self, *, bucket: str, expect_error: bool = False):
        """Upload a test file to the bucket."""
        with generate_testfile(
            file_owner=self.profile_name, prefix=TEST_FILE_PREFIX
        ) as master_test_file:
            test_file_key = os.path.basename(master_test_file.name)
            self.upload_file_multipart(
                bucket=bucket,
                key=str(test_file_key),
                file_path=master_test_file.name,
                expect_error=expect_error,
            )
        return test_file_key
