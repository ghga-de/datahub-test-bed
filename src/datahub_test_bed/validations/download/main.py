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

"""Main validations and tests module for file downloads."""

import logging
import os

from datahub_test_bed.validations.download.client import DownloadClient
from datahub_test_bed.validations.models import DownloadConfig
from datahub_test_bed.validations.utils import upload_test_file

logger = logging.getLogger(__name__)


def run_download_validation(config: DownloadConfig, object_key: str | bool):
    """Run the download validation."""
    if not object_key:
        logger.info("No object key provided. Uploading a test file.")
        uploader_client = DownloadClient(
            config=config,
            account=config.account_for_upload,
        )
        test_object_key, test_file_checksum = upload_test_file(
            client=uploader_client,
            bucket=config.bucket,
            file_size=config.test_file_size,
        )
        if not test_object_key:
            logger.error("Failed to upload the test file. Cancelling...")
            return

    downloader_client = DownloadClient(
        config=config, account=config.account_for_download
    )

    downloaded_file_path = os.path.join(config.output_dir, test_object_key)
    downloader_client.download_file(test_object_key, downloaded_file_path)

    if not os.path.exists(downloaded_file_path):
        logger.error("Failed to download the test file. Cancelling...")
        return

    logger.info("Download completed. Validating the downloaded file.")
    downloader_client.validate_downloaded_file(test_file_checksum, downloaded_file_path)

    logger.info("Download validation completed.")
