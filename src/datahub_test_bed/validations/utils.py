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

"""Utility functions for storage validations."""

import logging
import os
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

import botocore.exceptions

from datahub_test_bed.validations.exceptions import (
    UnexpectedHTTPStatusException,
)

PART_SIZE = 15728640
PART_COUNT = 2
TEST_FILE_PREFIX = "testfile_upload_"

logger = logging.getLogger("storage")


def log_unexpected_error(  # noqa: PLR0913
    operation: str,
    account_name: str,
    bucket: str,
    error_message: str,
    key=None,
    expect_error=False,
):
    """Log an error message with details."""
    if expect_error:
        logger.info(
            'Expected fail on operation "%s" to "%s" bucket using "%s". Reason: "%s"',
            operation,
            bucket,
            account_name,
            error_message,
        )
        return

    logger.error(
        '"%s" failed for "%s%s" bucket using "%s". Reason: "%s"',
        operation,
        bucket,
        f"/{key}" if key else "",
        account_name,
        error_message,
    )
    if any([("AccessDenied" in error_message), ("Forbidden" in error_message)]):
        logger.error(
            'Possible policy or ownership issue. Check that "%s" has correct permissions to "%s" for bucket: "%s".',
            account_name,
            operation,
            bucket,
        )


@contextmanager
def generate_testfile(client, prefix):
    """Generate a test file for upload."""
    file_prefix = f"{prefix}{client.profile_name}_"
    with NamedTemporaryFile(delete=False, prefix=file_prefix) as test_file:
        logger.info(
            'Created test file for "%s" at "%s"',
            client.profile_name,
            test_file.name,
        )
        test_file.write(b"\0" * (PART_SIZE * PART_COUNT))
        yield test_file


def upload_test_file(client, bucket, expect_error=False):
    """Upload a test file to the bucket."""
    with generate_testfile(client, TEST_FILE_PREFIX) as master_test_file:
        test_file_key = os.path.basename(master_test_file.name)
        client.upload_file_multipart(
            bucket=bucket,
            key=str(test_file_key),
            file_path=master_test_file.name,
            expect_error=expect_error,
        )
    return test_file_key


def get_error_message(
    error: botocore.exceptions.ClientError | UnexpectedHTTPStatusException,
) -> str:
    """Retrieve the error message from the botocore ClientError"""
    if isinstance(error, UnexpectedHTTPStatusException):
        return str(error)
    error_response = error.response["Error"]
    message = error_response.get("Message")
    if message is not None and message != "None":
        return message
    return error_response.get("Code", "Unknown error")
