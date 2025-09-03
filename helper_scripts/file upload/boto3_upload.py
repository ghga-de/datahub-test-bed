#!/usr/bin/env python3

"""
Author: Thomas Gerlach tgerlac1@uni-koeln.de
Modified by: Seyit Zor seyit.zor@embl.de
"""

import os
import time
import uuid

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "WHEN_REQUIRED"

# Konfiguration
ENDPOINT_URL = "<PLEASE FILL>"
BUCKET_NAME = "<PLEASE FILL>"
ACCESS_KEY = ""
SECRET_KEY = ""
TEST_FILE_PATH = "testfile.bin"  # Wird erstellt, falls nicht vorhanden
UPLOAD_DIR = "upload_logs"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Client erstellen
s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)


def generate_test_file(size_mb):
    """Erstellt eine Testdatei mit der angegebenen Größe in MB."""
    file_path = f"{TEST_FILE_PATH}"
    with open(file_path, "wb") as f:
        f.write(os.urandom(size_mb * 1024 * 1024))
    return file_path


def log_result(file_size_mb, part_size_mb, success, error=None):
    """Protokolliert das Ergebnis des Uploads."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_file = os.path.join(UPLOAD_DIR, "upload_results.txt")
    with open(log_file, "a") as f:
        f.write(
            f"[{timestamp}] {file_size_mb}MB | {part_size_mb}MB | {'Success' if success else 'Failed'} | {error}\n"
        )


def upload_file_in_parts(file_path, part_size_mb):
    """Uploadet eine Datei in Teilen (multipart upload)."""
    file_size = os.path.getsize(file_path)
    part_size = part_size_mb * 1024 * 1024
    total_parts = (file_size + part_size - 1) // part_size
    key = str(uuid.uuid4())
    # Multipart Upload starten
    try:
        response = s3_client.create_multipart_upload(Bucket=BUCKET_NAME, Key=key)
        upload_id = response["UploadId"]
    except ClientError as e:
        log_result(file_size // (1024 * 1024), part_size_mb, False, str(e))
        return

    parts = []
    with open(file_path, "rb") as f:
        for i in range(1, total_parts + 1):
            start = (i - 1) * part_size
            f.seek(start)
            part_data = f.read(part_size)
            if not part_data:
                break

            try:
                part_response = s3_client.upload_part(
                    Bucket=BUCKET_NAME,
                    Key=key,
                    PartNumber=i,
                    UploadId=upload_id,
                    Body=part_data,
                )
                parts.append({"PartNumber": i, "ETag": part_response["ETag"]})
            except (ClientError, EndpointConnectionError, ReadTimeoutError) as e:
                log_result(file_size // (1024 * 1024), part_size_mb, False, str(e))
                s3_client.abort_multipart_upload(
                    Bucket=BUCKET_NAME, Key=key, UploadId=upload_id
                )
                return

    # Upload abschließen
    try:
        s3_client.complete_multipart_upload(
            Bucket=BUCKET_NAME,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        log_result(file_size // (1024 * 1024), part_size_mb, True)
    except ClientError as e:
        log_result(file_size // (1024 * 1024), part_size_mb, False, str(e))
        s3_client.abort_multipart_upload(
            Bucket=BUCKET_NAME, Key=key, UploadId=upload_id
        )


def run_tests():
    """Führt Upload-Tests mit verschiedenen Dateigrößen und Part-Größen durch."""
    test_cases = [
        (1, 10),
        (10, 10),
        (100, 10),
        (1000, 10),
    ]

    for file_size_mb, part_size_mb in test_cases:
        print(f"Testing {file_size_mb}MB with {part_size_mb}MB parts...")
        file_path = generate_test_file(file_size_mb)
        upload_file_in_parts(file_path, part_size_mb)
        time.sleep(1)  # Vermeiden von Rate-Limiting


if __name__ == "__main__":
    run_tests()
