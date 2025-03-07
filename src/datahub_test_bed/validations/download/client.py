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

"""Client for file download."""

import hashlib
import logging
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import httpx

from datahub_test_bed.validations.models import AccountConfig, DownloadConfig
from datahub_test_bed.validations.storage.client import StorageClient

logger = logging.getLogger("download")


class DownloadClient(StorageClient):
    """A client for operations related to download."""

    def __init__(self, config: DownloadConfig, account: AccountConfig):
        super().__init__(s3_url_endpoint=config.s3_url_endpoint, account=account)
        self.config = config

    def download_file(self, object_key: str, local_path: str):
        """Download a file from a presigned URL."""
        now = datetime.now().astimezone().replace(microsecond=0)
        presigned_url = self.get_presigned_url_for_object(
            bucket=self.config.bucket,
            key=object_key,
            expiration=self.config.presigned_url_expiration,
        )
        if not presigned_url:
            logger.error("Presigned URL not found. Download cancelled.")
            return
        self.log_url_info(now, presigned_url)
        self.download_file_from_url(presigned_url, local_path)

    def download_file_from_url(self, url: str, local_path: str):
        """Download a file from a URL with error handling and log the duration."""
        start_time = datetime.now()
        try:
            logger.info(f"Downloading file from URL: {url}")
            with httpx.stream("GET", url) as response:
                response.raise_for_status()
                with open(local_path, "wb") as file:
                    for chunk in response.iter_bytes():
                        file.write(chunk)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            speed = round(
                (int(self.config.test_file_size) / (1024 * 1024)) / duration, 2
            )
            logger.info(
                'File downloaded successfully in "%s" seconds (%s MB/s).',
                duration,
                speed,
            )
        except httpx.HTTPStatusError as http_err:
            logger.error(http_err)

    def validate_downloaded_file(self, uploaded_file_sha256, file_path: str):
        """Validate the downloaded file content."""
        with open(file_path, "rb") as f:
            file_sha256 = hashlib.sha256(f.read()).hexdigest()
        if file_sha256 == uploaded_file_sha256:
            logger.info("Downloaded file content matches the uploaded file.")
        else:
            logger.error("Downloaded file content does not match the uploaded file.")
            logger.error(f"Uploaded file SHA256: {uploaded_file_sha256}")
            logger.error(f"Downloaded file SHA256: {file_sha256}")

    def log_url_info(self, presigned_url_requested_time, presigned_url: str):
        """Log the presigned URL info."""
        logger.info(
            "Presigned URL requested at %s UTC with %s seconds expiration.",
            presigned_url_requested_time,
            self.config.presigned_url_expiration,
        )
        parsed_url = urlparse(presigned_url)
        query_params = parse_qs(parsed_url.query)
        expires = query_params.get("Expires", [None])[0]
        if expires:
            expires_timestamp = int(expires)
            expires_local_time = datetime.fromtimestamp(
                expires_timestamp, tz=UTC
            ).astimezone()
            logger.info(f"Created presigned URL expires at: {expires_local_time} UTC")
