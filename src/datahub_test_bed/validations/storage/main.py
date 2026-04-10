# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
import tempfile

from datahub_test_bed.validations.models import Buckets, StorageConfig
from datahub_test_bed.validations.storage.client import StorageClient
from datahub_test_bed.validations.utils import TEST_FILE_PREFIX

logger = logging.getLogger(__name__)

BUCKET_ACCESS = {
    "inbox_bucket": {
        "master": False,
        "ucs": False,
        "dhfs": False,
        "ifrs": True,
        "dcs": True,
    },
    "interrogation_bucket": {
        "master": False,
        "dhfs": False,
        "ifrs": False,
        "ucs": True,
        "dcs": True,
    },
    "permanent_bucket": {
        "master": False,
        "ifrs": False,
        "ucs": True,
        "dcs": True,
        "dhfs": True,
    },
    "outbox_bucket": {
        "master": False,
        "ifrs": False,
        "dcs": False,
        "ucs": True,
        "dhfs": True,
    },
}


def check_bucket_accessibility(buckets: Buckets, clients: dict):
    """Check the accessibility of the buckets."""
    logger.info("Checking bucket accessibility")

    for bucket_name, access in BUCKET_ACCESS.items():
        bucket = getattr(buckets, bucket_name)
        for client_name, expect_error in access.items():
            clients[client_name].head_bucket(bucket, expect_error=expect_error)


def check_list_bucket_objects(clients: dict, buckets: Buckets):
    """Check listing of objects in the bucket."""
    logger.info("Checking listing of objects in buckets")

    for bucket_name, access in BUCKET_ACCESS.items():
        bucket = getattr(buckets, bucket_name)
        for client_name, expect_error in access.items():
            clients[client_name].list_all_object_in_bucket(
                bucket=bucket, expect_error=expect_error
            )


def check_uploads_expected_to_fail(clients, buckets):
    """Try to upload files that are expected to be denied by policies.

    IFRS should not be able to upload to the inbox and interrogation bucket.
    DCS should not be able to upload to any bucket.
    UCS should not be able to upload to the interrogation, permanent and outbox bucket.
    DHFS should not be able to upload to the inbox, permanent and outbox bucket.
    """
    expected_upload_failures = {
        "ifrs": (
            buckets.inbox_bucket,
            buckets.interrogation_bucket,
        ),
        "dcs": (
            buckets.inbox_bucket,
            buckets.interrogation_bucket,
            buckets.permanent_bucket,
            buckets.outbox_bucket,
        ),
        "ucs": (
            buckets.interrogation_bucket,
            buckets.permanent_bucket,
            buckets.outbox_bucket,
        ),
        "dhfs": (
            buckets.inbox_bucket,
            buckets.permanent_bucket,
            buckets.outbox_bucket,
        ),
    }

    for client_name, bucket_list in expected_upload_failures.items():
        for bucket in bucket_list:
            clients[client_name].upload_test_file(
                bucket=bucket,
                expect_error=True,
            )


def check_dhfs_transfer(client_dhfs, inbox_bucket, inbox_key, interrogation_bucket):
    """DHFS downloads a file from inbox via presigned URL and re-uploads to interrogation.

    This is distinct from a server-side copy: DHFS uses get_object (via presigned URL)
    to download from inbox, then performs a multipart upload to interrogation.
    """
    logger.info(
        'DHFS downloading "%s" from "%s" via presigned URL and re-uploading to "%s"',
        inbox_key,
        inbox_bucket,
        interrogation_bucket,
    )
    content = client_dhfs.get_object_via_presigned_url(inbox_bucket, inbox_key)
    if content is None:
        logger.error(
            'DHFS failed to download "%s" from "%s" via presigned URL',
            inbox_key,
            inbox_bucket,
        )
        return
    dst_key = f"{inbox_key}-transferred-by-{client_dhfs.profile_name}"
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(content)
        tmp.flush()
        client_dhfs.upload_file_multipart(
            file_path=tmp.name,
            key=dst_key,
            bucket=interrogation_bucket,
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
        buckets.inbox_bucket,
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
        "ucs": StorageClient(
            s3_url_endpoint=config.s3_url_endpoint, account=config.accounts.ucs
        ),
        "dhfs": StorageClient(
            s3_url_endpoint=config.s3_url_endpoint, account=config.accounts.dhfs
        ),
    }

    # ----- CHECK BUCKET ACCESSIBILITY -----

    check_bucket_accessibility(buckets=config.buckets, clients=clients)

    # ----- CHECK LISTING OF OBJECTS IN BUCKETS -----

    check_list_bucket_objects(clients=clients, buckets=config.buckets)

    # ----- CHECK UPLOADS EXPECTED TO FAIL -----

    check_uploads_expected_to_fail(clients=clients, buckets=config.buckets)

    # ----- MULTIPART UPLOAD TEST FILES -----

    # UCS should be able to write/upload to the inbox bucket
    ucs_test_file_inbox = clients["ucs"].upload_test_file(
        bucket=config.buckets.inbox_bucket,
    )

    # DHFS should be able to write/upload to the interrogation
    dhfs_test_file_interrogation = clients["dhfs"].upload_test_file(
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

    # ----- CHECK DHFS RE-UPLOAD (download from inbox & upload to interrogation) -----

    # DHFS downloads the UCS-uploaded file from the inbox bucket via presigned URL
    # and uploads to the interrogation bucket
    check_dhfs_transfer(
        client_dhfs=clients["dhfs"],
        inbox_bucket=config.buckets.inbox_bucket,
        inbox_key=ucs_test_file_inbox,
        interrogation_bucket=config.buckets.interrogation_bucket,
    )

    # ----- CHECK MULTIPART FILE COPY -----

    # Run two copy scenarios to validate both the policies and object ownership issues.

    # 1. Multipart copy a file uploaded/owned by the DHFS account using the IFRS account.
    # This is the desired scenario where the IFRS account should be able to copy files
    # uploaded by UCS. This could fail for two reasons:
    # a. The IFRS account is not allowed to copy files from the interrogation bucket.
    # b. The IFRS account is not allowed to copy files owned by the UCS account.
    # It is not possible to distinguish between these two causes due to a known bug in some
    # Ceph versions: https://tracker.ceph.com/issues/61954
    # If this fails, it is recommended to ensure that the bucket policies are correctly set
    # and to check for object ownership issues.
    check_copy_file(
        client_owner=clients["dhfs"],
        client_copier=clients["ifrs"],
        bucket_from=config.buckets.interrogation_bucket,
        bucket_to=config.buckets.permanent_bucket,
        object_key=dhfs_test_file_interrogation,
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

    # UCS deletes the file from the inbox bucket that it uploaded
    clients["ucs"].delete_object(config.buckets.inbox_bucket, ucs_test_file_inbox)

    # IFRS deletes the files from the interrogation that DHFS uploaded
    clients["ifrs"].delete_object(
        config.buckets.interrogation_bucket, dhfs_test_file_interrogation
    )

    # IFRS deletes the files from the permanent that it copied
    clients["ifrs"].delete_object(
        config.buckets.permanent_bucket, ifrs_test_file_permanent
    )

    # DCS deletes the file from the outbox that IFRS staged
    clients["dcs"].delete_object(config.buckets.outbox_bucket, ifrs_test_file_outbox)

    # ----- DELETE ALL THE TEST FILES -----

    delete_all_test_files(clients, config.buckets)

    logger.info("Storage validations have been completed.")
    logger.info("----------")
    logger.info("Please check the ERROR logs for any issues")
    logger.info("----------")
