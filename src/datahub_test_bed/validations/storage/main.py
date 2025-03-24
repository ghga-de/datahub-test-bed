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

"""Main module for the storage validations."""

import logging

from datahub_test_bed.validations.models import Buckets, StorageConfig
from datahub_test_bed.validations.storage.client import StorageClient
from datahub_test_bed.validations.utils import TEST_FILE_PREFIX

logger = logging.getLogger(__name__)


def check_bucket_accessibility(buckets: Buckets, clients: dict):
    """Check the accessibility of the buckets.

    Master and IFRS accounts should have access to all buckets.
    DCS account should have access to the outbox bucket.
    """
    logger.info("Checking bucket accessibility")
    for bucket in [
        buckets.interrogation_bucket,
        buckets.permanent_bucket,
        buckets.outbox_bucket,
    ]:
        clients["master"].head_bucket(bucket)
        clients["ifrs"].head_bucket(bucket)
        clients["dcs"].head_bucket(
            bucket, expect_error=(bucket != buckets.outbox_bucket)
        )


def check_list_bucket_objects(clients: dict, buckets: Buckets):
    """Check listing of objects in the bucket.

    Master and IFRS account should be able to list objects in all the buckets.
    DCS account should be able to list objects in the outbox bucket.
    """
    logger.info("Checking listing of objects in buckets")
    for bucket in [
        buckets.interrogation_bucket,
        buckets.permanent_bucket,
        buckets.outbox_bucket,
    ]:
        clients["master"].list_all_object_in_bucket(bucket=bucket)
        clients["ifrs"].list_all_object_in_bucket(bucket=bucket)
        clients["dcs"].list_all_object_in_bucket(
            bucket=bucket, expect_error=(bucket != buckets.outbox_bucket)
        )


def check_uploads_expected_to_fail(clients, buckets):
    """Try to upload files that are expected to be denied by policies.

    IFRS should not be able to upload to the interrogation bucket.
    DCS should not be able to upload to any bucket.
    """
    clients["ifrs"].upload_test_file(
        bucket=buckets.interrogation_bucket,
        expect_error=True,
    )

    for bucket in [
        buckets.interrogation_bucket,
        buckets.permanent_bucket,
        buckets.outbox_bucket,
    ]:
        clients["dcs"].upload_test_file(
            bucket=bucket,
            expect_error=True,
        )


def check_copy_file(client_owner, client_copier, bucket_from, bucket_to, object_key):
    """Copy a file from one bucket to another, considering the ownership."""
    logger.info(
        'Copying the file owned by "%s" account from "%s" to "%s" using "%s"',
        client_owner.profile_name,
        client_copier.profile_name,
        bucket_from,
        bucket_to,
    )
    object_dst_key = f"{object_key}-copied-w-{client_copier.profile_name}"
    client_copier.copy_file_multipart(
        bucket_from,
        object_key,
        bucket_to,
        object_dst_key,
    )


def delete_all_test_files(clients, buckets):
    """Delete all the test files uploaded during the validations."""
    for b in [
        buckets.interrogation_bucket,
        buckets.permanent_bucket,
        buckets.outbox_bucket,
    ]:
        objects = clients["master"].list_all_object_in_bucket(
            bucket=b, prefix=TEST_FILE_PREFIX, return_objects=True
        )
        for obj in objects:
            k = obj["Key"]
            if k.startswith(TEST_FILE_PREFIX):
                clients["master"].delete_object(b, k)


def run_validations(config: StorageConfig):
    """Run the storage validations."""
    logger.info("Running storage validations")
    clients = {
        "master": StorageClient(
            s3_url_endpoint=config.s3_url_endpoint, account=config.accounts.master
        ),
        "ifrs": StorageClient(
            s3_url_endpoint=config.s3_url_endpoint, account=config.accounts.ifrs
        ),
        "dcs": StorageClient(
            s3_url_endpoint=config.s3_url_endpoint, account=config.accounts.dcs
        ),
    }

    # ----- CHECK BUCKET ACCESSIBILITY -----

    check_bucket_accessibility(buckets=config.buckets, clients=clients)

    # ----- CHECK LISTING OF OBJECTS IN BUCKETS -----

    check_list_bucket_objects(clients=clients, buckets=config.buckets)

    # ----- MULTIPART UPLOAD TEST FILES -----

    # Upload test files with Master account as it is used by Data Stewards to upload
    master_test_file_interrogation = clients["master"].upload_test_file(
        bucket=config.buckets.interrogation_bucket,
    )

    # IFRS should be able to write/upload to the permanent bucket
    ifrs_test_file_permanent = clients["ifrs"].upload_test_file(
        bucket=config.buckets.permanent_bucket,
    )

    # IFRS should be able to write/upload to the outbox bucket
    ifrs_test_file_outbox = clients["ifrs"].upload_test_file(
        bucket=config.buckets.outbox_bucket,
    )

    # ----- CHECK UPLOADS EXPECTED TO FAIL -----

    check_uploads_expected_to_fail(clients=clients, buckets=config.buckets)

    # ----- CHECK MULTIPART FILE COPY -----

    # Run two copy scenarios to check both the policies and object ownership issues.

    # 1. Multipart copy a file uploaded/owned by the Master account using the IFRS account.
    # This is the desired scenario where the IFRS account should be able to copy files
    # uploaded by Data Stewards using the Master account. This could fail for two reasons:
    # a. The IFRS account is not allowed to copy files from the interrogation bucket.
    # b. The IFRS account is not allowed to copy files owned by the Master account.
    # It is not possible to distinguish between these two causes due to a known bug in some
    # Ceph versions: https://tracker.ceph.com/issues/61954
    # If this fails, it is recommended to ensure that the bucket policies are correctly set
    # and to check for object ownership issues.
    check_copy_file(
        client_owner=clients["master"],
        client_copier=clients["ifrs"],
        bucket_from=config.buckets.interrogation_bucket,
        bucket_to=config.buckets.permanent_bucket,
        object_key=master_test_file_interrogation,
    )

    # 2. Multipart copy file that uploaded/owned by IFRS account itself
    # This is the desired scenario where IFRS account should be able to copy the file.
    # IFRS account first copies the file from the interrogration bucket to the permanent bucket.
    # Then copies the file from the permanent bucket to the outbox bucket.
    check_copy_file(
        client_owner=clients["ifrs"],
        client_copier=clients["ifrs"],
        bucket_from=config.buckets.permanent_bucket,
        bucket_to=config.buckets.outbox_bucket,
        object_key=ifrs_test_file_permanent,
    )

    # ----- CHECK PRESIGNED URL FOR FILE DOWNLOAD -----

    clients["dcs"].get_presigned_url_for_object(
        config.buckets.outbox_bucket, ifrs_test_file_outbox
    )

    # ----- CHECK DELETION OF OBJECTS -----

    # Delete the test files uploaded during the validations
    # using the responsible accounts for cleaning their respective buckets

    clients["ifrs"].delete_object(
        config.buckets.interrogation_bucket, master_test_file_interrogation
    )
    clients["ifrs"].delete_object(
        config.buckets.permanent_bucket, ifrs_test_file_permanent
    )
    clients["dcs"].delete_object(config.buckets.outbox_bucket, ifrs_test_file_outbox)

    # ----- DELETE ALL THE TEST FILES -----

    delete_all_test_files(clients, config.buckets)

    logger.info("Storage validations have been completed.")
    logger.info("----------")
    logger.info("Please check the ERROR logs for any issues")
    logger.info("----------")
