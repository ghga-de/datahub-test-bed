#!/usr/bin/env python3
# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Custom script to encrypt data using Crypt4GH and directly uploading it to S3
objectstorage.
"""

import asyncio
import base64
import hashlib
import logging
import math
import os
import subprocess  # nosec
import sys
from functools import partial
from io import BufferedReader
from pathlib import Path
from time import time
from typing import Any, Generator
from uuid import uuid4

import crypt4gh.header  # type: ignore
import crypt4gh.keys  # type: ignore
import crypt4gh.lib  # type: ignore
import requests  # type: ignore
import typer
import yaml
from ghga_connector.core.file_operations import read_file_parts
from ghga_connector.core.session import RequestsSession
from hexkit.providers.s3 import S3Config, S3ObjectStorage  # type: ignore
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt
from pydantic import BaseSettings, Field, SecretStr


def configure_session() -> requests.Session:
    """Configure session with exponential backoff retry"""
    RequestsSession.configure(6)
    return RequestsSession.session


LOGGER = logging.getLogger("s3_upload")
LOGGER.setLevel(logging.INFO)
PART_SIZE = 16 * 1024**2
SESSION = configure_session()
upload_times = []
download_times = []


def expand_env_vars_in_path(path: Path) -> Path:
    """Expand environment variables in a Path."""

    with subprocess.Popen(  # nosec
        f"realpath {path}", shell=True, stdout=subprocess.PIPE
    ) as process:
        if process.wait() != 0 or not process.stdout:
            raise RuntimeError(f"Parsing of path failed: {path}")

        output = process.stdout.read().decode("utf-8").strip()

    return Path(output)


class Config(BaseSettings):
    """
    Required options from a config file named .upload.yaml placed in
    the current working dir or the home dir.
    """

    s3_endpoint_url: SecretStr = Field(..., description="URL of the S3 server")
    s3_access_key_id: SecretStr = Field(
        ..., description="Access key ID for the S3 server"
    )
    s3_secret_access_key: SecretStr = Field(
        ..., description="Secret access key for the S3 server"
    )
    bucket_id: str = Field(
        ..., description="Bucket id where the encrypted, uploaded file is stored"
    )
    part_size: int = Field(
        16, description="Upload part size in MiB. Has to be between 5 and 5120."
    )


class Checksums:
    """Container for checksum calculation"""

    def __init__(self):
        self.unencrypted_sha256 = hashlib.sha256()
        self.encrypted_md5: list[str] = []
        self.encrypted_sha256: list[str] = []

    def __repr__(self) -> str:
        return (
            f"Unencrypted: {self.unencrypted_sha256.hexdigest()}\n"
            + f"Encrypted MD5: {self.encrypted_md5}\n"
            + f"Encrypted SHA256: {self.encrypted_sha256}"
        )

    def get(self):
        """Return all checksums at the end of processing"""
        return (
            self.unencrypted_sha256.hexdigest(),
            self.encrypted_md5,
            self.encrypted_sha256,
        )

    def update_unencrypted(self, part: bytes):
        """Update checksum for unencrypted file"""
        self.unencrypted_sha256.update(part)

    def update_encrypted(self, part: bytes):
        """Update encrypted part checksums"""
        self.encrypted_md5.append(hashlib.md5(part, usedforsecurity=False).hexdigest())
        self.encrypted_sha256.append(hashlib.sha256(part).hexdigest())


class ChunkedUploader:
    """Handler class dealing with upload functionality"""

    def __init__(
        self, input_path: Path, config: Config, unencrypted_file_size: int
    ) -> None:
        self.config = config
        self.input_path = input_path
        self.encryptor = Encryptor(self.config.part_size)
        self.file_id = str(uuid4())
        self.unencrypted_file_size = unencrypted_file_size
        self.encrypted_file_size = 0

    async def encrypt_and_upload(self):
        """Delegate encryption and perform multipart upload"""

        # compute encrypted_file_size
        num_segments = math.ceil(self.unencrypted_file_size / crypt4gh.lib.SEGMENT_SIZE)
        encrypted_file_size = self.unencrypted_file_size + num_segments * 28
        num_parts = math.ceil(encrypted_file_size / self.config.part_size)

        start = time()

        with open(self.input_path, "rb") as file:
            async with MultipartUpload(
                config=self.config,
                file_id=self.file_id,
                encrypted_file_size=encrypted_file_size,
                part_size=self.config.part_size,
            ) as upload:
                LOGGER.info("UPLOAD: Initialized file upload for %s.", upload.file_id)
                for part_number, part in enumerate(
                    self.encryptor.process_file(file=file), start=1
                ):
                    await upload.send_part(part_number=part_number, part=part)

                    delta_for_part = time() - start
                    avg_speed = (
                        part_number
                        * (self.config.part_size / 1024**2)
                        / delta_for_part
                    )
                    LOGGER.info(
                        "UPLOAD: Processing Part No. %i/%i (%.2f MiB/s)",
                        part_number,
                        num_parts,
                        avg_speed,
                    )
                    upload_times.append(delta_for_part)
                if encrypted_file_size != self.encryptor.encrypted_file_size:
                    raise ValueError(
                        "Mismatch between actual and theoretical encrypted part size:\n"
                        + f"Is: {self.encryptor.encrypted_file_size}\n"
                        + f"Should be: {encrypted_file_size}"
                    )

                delta = time() - start
                LOGGER.info(
                    "UPLOAD: Uploaded %.2f MiB in %.2f sec (encrypted size) for %s",
                    encrypted_file_size / 1024**2,
                    delta,
                    upload.file_id,
                )


class ChunkedDownloader:
    """Handler class dealing with download functionality"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        config: Config,
        file_id: str,
        encrypted_file_size: int,
        file_secret: bytes,
        part_size: int,
        target_checksums: Checksums,
    ) -> None:
        self.config = config
        self.storage = objectstorage(self.config)
        self.file_id = file_id
        self.file_size = encrypted_file_size
        self.file_secret = file_secret
        self.part_size = part_size
        self.target_checksums = target_checksums

    def _download_parts(self, download_url):
        """Download file parts"""

        for part_no, (start, stop) in enumerate(
            get_ranges(file_size=self.file_size, part_size=self.config.part_size),
            start=1,
        ):
            headers = {"Range": f"bytes={start}-{stop}"}
            LOGGER.debug("Downloading part number %i. %s", part_no, headers)
            response = SESSION.get(download_url, timeout=60, headers=headers)

            yield response.content

    async def download(self):
        """Download file in parts and validate checksums"""
        LOGGER.info("DOWNLOAD: Downloading file %s for validation.", self.file_id)
        download_url = await self.storage.get_object_download_url(
            bucket_id=self.config.bucket_id, object_id=self.file_id
        )
        num_parts = math.ceil(self.file_size / self.part_size)
        decryptor = Decryptor(
            file_secret=self.file_secret, num_parts=num_parts, part_size=self.part_size
        )
        download_func = partial(self._download_parts, download_url=download_url)
        start = time()
        decryptor.process_parts(download_func)

        delta = time() - start
        LOGGER.info(
            "DOWNLOAD: Downloaded %.2f MiB in %.2f sec (encrypted size)",
            self.file_size / (1024**2),
            delta,
        )
        self.validate_checksums(checkums=decryptor.checksums)

    def validate_checksums(self, checkums: Checksums):
        """Confirm checksums for upload and download match"""
        if not self.target_checksums.get() == checkums.get():
            raise ValueError(
                f"Checksum mismatch:\nUpload:\n{checkums}\nDownload:\n{self.target_checksums}"
            )
        LOGGER.info("DOWNLOAD: Succesfully validated checksums for %s.", self.file_id)


class Decryptor:
    """Handles on the fly decryption and checksum calculation"""

    def __init__(self, file_secret: bytes, num_parts: int, part_size: int) -> None:
        self.checksums = Checksums()
        self.file_secret = file_secret
        self.num_parts = num_parts
        self.part_size = part_size

    def _decrypt(self, part: bytes):
        """Decrypt file part"""
        segments, incomplete_segment = get_segments(
            part=part, segment_size=crypt4gh.lib.CIPHER_SEGMENT_SIZE
        )

        decrypted_segments = []
        for segment in segments:
            decrypted_segments.append(self._decrypt_segment(segment))

        return b"".join(decrypted_segments), incomplete_segment

    def _decrypt_segment(self, segment: bytes):
        """Decrypt single ciphersegment"""
        return crypt4gh.lib.decrypt_block(
            ciphersegment=segment, session_keys=[self.file_secret]
        )

    def process_parts(self, download_files: partial[Generator[bytes, None, None]]):
        """download and decrypt file parts."""
        unprocessed_bytes = b""
        download_buffer = b""
        start = time()

        for part_number, file_part in enumerate(download_files()):
            # process encrypted
            self.checksums.update_encrypted(file_part)
            unprocessed_bytes += file_part

            # decrypt in chunks
            decrypted_bytes, unprocessed_bytes = self._decrypt(unprocessed_bytes)
            download_buffer += decrypted_bytes

            # update checksums and yield if part size
            if len(download_buffer) >= self.part_size:
                current_part = download_buffer[: self.part_size]
                self.checksums.update_unencrypted(current_part)
                download_buffer = download_buffer[self.part_size :]

            delta_for_part = time() - start
            avg_speed = (part_number * (self.part_size / 1024**2)) / delta_for_part
            LOGGER.info(
                "DOWNLOAD: Downloading Part No. %i/%i (%.2f MiB/s)",
                part_number + 1,
                self.num_parts,
                avg_speed,
            )
            download_times.append(delta_for_part)

        # process dangling bytes
        if unprocessed_bytes:
            download_buffer += self._decrypt_segment(unprocessed_bytes)

        while len(download_buffer) >= self.part_size:
            current_part = download_buffer[: self.part_size]
            self.checksums.update_unencrypted(current_part)
            download_buffer = download_buffer[self.part_size :]

        if download_buffer:
            self.checksums.update_unencrypted(download_buffer)


class Encryptor:
    """Handles on the fly encryption and checksum calculation"""

    def __init__(self, part_size: int):
        self.part_size = part_size
        self.checksums = Checksums()
        self.file_secret = os.urandom(32)
        self.encrypted_file_size = 0

    def _encrypt(self, part: bytes):
        """Encrypt file part using secret"""
        segments, incomplete_segment = get_segments(
            part=part, segment_size=crypt4gh.lib.SEGMENT_SIZE
        )

        encrypted_segments = []
        for segment in segments:
            encrypted_segments.append(self._encrypt_segment(segment))

        return b"".join(encrypted_segments), incomplete_segment

    def _encrypt_segment(self, segment=bytes):
        """Encrypt one single segment"""
        nonce = os.urandom(12)
        encrypted_data = crypto_aead_chacha20poly1305_ietf_encrypt(
            segment, None, nonce, self.file_secret
        )  # no aad
        return nonce + encrypted_data

    # type annotation for file parts, should be generator
    def process_file(self, file: BufferedReader):
        """Encrypt and upload file parts."""
        unprocessed_bytes = b""
        upload_buffer = b""

        for file_part in read_file_parts(file=file, part_size=self.part_size):
            # process unencrypted
            self.checksums.update_unencrypted(file_part)
            unprocessed_bytes += file_part

            # encrypt in chunks
            encrypted_bytes, unprocessed_bytes = self._encrypt(unprocessed_bytes)
            upload_buffer += encrypted_bytes

            # update checksums and yield if part size
            if len(upload_buffer) >= self.part_size:
                current_part = upload_buffer[: self.part_size]
                self.checksums.update_encrypted(current_part)
                self.encrypted_file_size += self.part_size
                yield current_part
                upload_buffer = upload_buffer[self.part_size :]

        # process dangling bytes
        if unprocessed_bytes:
            upload_buffer += self._encrypt_segment(unprocessed_bytes)

        while len(upload_buffer) >= self.part_size:
            current_part = upload_buffer[: self.part_size]
            self.checksums.update_encrypted(current_part)
            self.encrypted_file_size += self.part_size
            yield current_part
            upload_buffer = upload_buffer[self.part_size :]

        if upload_buffer:
            self.checksums.update_encrypted(upload_buffer)
            self.encrypted_file_size += len(upload_buffer)
            yield upload_buffer


def summarize(  # pylint: disable=too-many-arguments
    elapsed: float,
    file_uuid: str,
    original_path: Path,
    part_size: int,
    file_secret: bytes,
    unencrypted_size: int,
    encrypted_size: int,
):
    """Log overview information"""

    output: dict[str, Any] = {}
    output["Elapsed time"] = f"{elapsed:.2f} seconds"
    output["Avg Part Upload"] = f"{sum(upload_times)/len(upload_times):.2f} seconds"
    output[
        "Fastest, Slowest Upload"
    ] = f"{min(upload_times):.2f} seconds, {max(upload_times):.2f} seconds"
    output[
        "Avg Part Download"
    ] = f"{sum(download_times)/len(download_times):.2f} seconds"
    output[
        "Fastest, Slowest Download"
    ] = f"{min(download_times):.2f} seconds, {max(download_times):.2f} seconds"
    output["File UUID"] = file_uuid
    output["Original filesystem path"] = str(original_path.resolve())
    output["Part Size"] = f"{part_size // 1024**2} MiB"
    output["Unencrypted file size"] = f"{unencrypted_size} bytes"
    output[
        "Encrypted file size"
    ] = f"{encrypted_size} bytes ({100 * (encrypted_size / unencrypted_size - 1):.2f}% change)"
    output["Symmetric file encryption secret"] = base64.b64encode(file_secret).decode(
        "utf-8"
    )

    LOGGER.info("SUMMARY:")
    for key, val in output.items():
        LOGGER.info("\t %s: %s", key, val)


class MultipartUpload:
    """Context manager to handle init + complete/abort for S3 multipart upload"""

    def __init__(
        self, config: Config, file_id: str, encrypted_file_size: int, part_size: int
    ) -> None:
        self.config = config
        self.storage = objectstorage(config=self.config)
        self.file_id = file_id
        self.file_size = encrypted_file_size
        self.part_size = part_size
        self.upload_id = ""

    async def __aenter__(self):
        """Start multipart upload"""
        self.upload_id = await self.storage.init_multipart_upload(
            bucket_id=self.config.bucket_id, object_id=self.file_id
        )
        return self

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        """Complete or clean up multipart upload"""
        try:
            await self.storage.complete_multipart_upload(
                upload_id=self.upload_id,
                bucket_id=self.config.bucket_id,
                object_id=self.file_id,
                anticipated_part_quantity=math.ceil(self.file_size / self.part_size),
                anticipated_part_size=self.part_size,
            )
        except (Exception, KeyboardInterrupt) as exc:  # pylint: disable=broad-except
            await self.storage.abort_multipart_upload(
                upload_id=self.upload_id,
                bucket_id=self.config.bucket_id,
                object_id=self.file_id,
            )
            raise exc

    async def send_part(self, part: bytes, part_number: int):
        """Handle upload of one file part"""
        try:
            upload_url = await self.storage.get_part_upload_url(
                upload_id=self.upload_id,
                bucket_id=self.config.bucket_id,
                object_id=self.file_id,
                part_number=part_number,
            )
            SESSION.put(url=upload_url, data=part)
        except (  # pylint: disable=broad-except
            Exception,
            KeyboardInterrupt,
        ) as exc:
            await self.storage.abort_multipart_upload(
                upload_id=self.upload_id,
                bucket_id=self.config.bucket_id,
                object_id=self.file_id,
            )
            raise exc


def objectstorage(config: Config):
    """Configure S3 and return S3 DAO"""
    s3_config = S3Config(
        s3_endpoint_url=config.s3_endpoint_url.get_secret_value(),
        s3_access_key_id=config.s3_access_key_id.get_secret_value(),
        s3_secret_access_key=config.s3_secret_access_key.get_secret_value(),
    )
    return S3ObjectStorage(config=s3_config)


def get_segments(part: bytes, segment_size: int):
    """Chunk part into cipher segments"""
    num_segments = len(part) / segment_size
    full_segments = int(num_segments)
    segments = [
        part[i * segment_size : (i + 1) * segment_size] for i in range(full_segments)
    ]

    # check if we have a remainder of bytes that we need to handle,
    # i.e. non-matching boundaries between part and cipher segment size
    incomplete_segment = b""
    partial_segment_idx = math.ceil(num_segments)
    if partial_segment_idx != full_segments:
        incomplete_segment = part[full_segments * segment_size :]
    return segments, incomplete_segment


def get_ranges(file_size: int, part_size: int):
    """Calculate part ranges"""
    num_parts = file_size / part_size
    num_parts_floor = int(num_parts)

    byte_ranges = [
        (part_size * part_no, part_size * (part_no + 1) - 1)
        for part_no in range(num_parts_floor)
    ]
    if math.ceil(num_parts) != num_parts_floor:
        byte_ranges.append((part_size * num_parts_floor, file_size - 1))

    return byte_ranges


def handle_superficial_error(msg: str):
    """Don't want user dealing with stacktrace on simple input/output issues, log instead"""
    LOGGER.critical(msg)
    sys.exit(-1)


def check_adjust_part_size(config: Config, file_size: int):
    """
    Convert specified part size from MiB to bytes, check if it needs adjustment and
    adjust accordingly
    """
    lower_bound = 5 * 1024**2
    upper_bound = 5 * 1024**3
    part_size = config.part_size * 1024**2

    # clamp user input part sizes
    if part_size < lower_bound:
        part_size = lower_bound
    elif part_size > upper_bound:
        part_size = upper_bound

    # fixed list for now, maybe change to something more meaningful
    sizes_mib = [2**x for x in range(3, 13)]
    sizes = [size * 1024**2 for size in sizes_mib]

    # encryption will cause growth of ~ 0.0427%, so assume we might
    # need five more parts for this check
    if file_size / part_size > 9_995:
        for candidate_size in sizes:
            if candidate_size > part_size and file_size / candidate_size <= 9_995:
                part_size = candidate_size
                break
        else:
            raise ValueError(
                "Could not find a valid part size that would allow to upload all file parts"
            )

    if part_size / 1024**2 != config.part_size:
        LOGGER.info(
            "Part size was adjusted from %sMiB to %sMiB.",
            config.part_size,
            part_size / 1024**2,
        )

    # need to set this either way as we convert MiB to bytes
    config.part_size = part_size


def main(
    input_path: Path = typer.Option(..., help="Local path of the input file"),
    config_path: Path = typer.Option(..., help="Path to a config YAML."),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Display info for individual file parts"
    ),
    debug: bool = typer.Option(
        False, "--debug", "-d", help="Enable debug-level logging"
    ),
):
    """
    Custom script to encrypt data using Crypt4GH and directly uploading it to S3
    objectstorage.
    """

    config = load_config_yaml(config_path)

    asyncio.run(
        async_main(input_path=input_path, config=config, verbose=verbose, debug=debug)
    )


def load_config_yaml(path: Path) -> Config:
    """Load config parameters from the specified YAML file."""

    with open(path, "r", encoding="utf-8") as config_file:
        config_dict = yaml.safe_load(config_file)
    return Config(**config_dict)


def filter_part_logs(record: logging.LogRecord) -> bool:
    """Filter out part-level logs if verbose is disabled, allow all else"""
    return "Part No." not in record.msg


async def async_main(input_path: Path, config: Config, verbose: bool, debug: bool):
    """
    Run encryption, upload and validation.
    """

    if not input_path.exists():
        msg = f"No such file: {input_path.resolve()}"
        handle_superficial_error(msg=msg)

    if input_path.is_dir():
        msg = f"File location points to a directory: {input_path.resolve()}"
        handle_superficial_error(msg=msg)

    file_size = input_path.stat().st_size
    check_adjust_part_size(config=config, file_size=file_size)

    if not verbose:
        LOGGER.addFilter(filter_part_logs)

    if debug:
        LOGGER.setLevel(logging.DEBUG)

    start = time()

    uploader = ChunkedUploader(
        input_path=input_path,
        config=config,
        unencrypted_file_size=file_size,
    )
    await uploader.encrypt_and_upload()

    downloader = ChunkedDownloader(
        config=config,
        file_id=uploader.file_id,
        encrypted_file_size=uploader.encryptor.encrypted_file_size,
        file_secret=uploader.encryptor.file_secret,
        part_size=config.part_size,
        target_checksums=uploader.encryptor.checksums,
    )
    await downloader.download()

    elapsed = time() - start

    summarize(
        elapsed=elapsed,
        file_uuid=uploader.file_id,
        original_path=input_path,
        part_size=config.part_size,
        file_secret=uploader.encryptor.file_secret,
        unencrypted_size=file_size,
        encrypted_size=uploader.encryptor.encrypted_file_size,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(message)s",
        datefmt="%d/%m/%Y %H:%M:%S",
    )
    typer.run(main)
