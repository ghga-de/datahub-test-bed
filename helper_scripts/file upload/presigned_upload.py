#!/usr/bin/env python3

"""Perform multipart uploads to an S3-compatible storage service using presigned URLs."""

import os
import sys
import uuid

import boto3
import requests
from botocore.exceptions import ClientError

ENDPOINT_URL = "<PLEASE FILL>"
BUCKET_NAME = "<PLEASE FILL>"
ACCESS_KEY = ""
SECRET_KEY = ""
FILE_PATH = "TESTFILE.bin"


def generate_test_file(size_mb):
    """Generate file in MB."""
    file_path = f"{FILE_PATH}"
    with open(file_path, "wb") as f:
        f.write(os.urandom(size_mb * 1024 * 1024))
    return file_path


def multipart_upload_with_presigned_urls(
    file_path, bucket_name, object_key, chunk_size
):
    """Upload a file using multipart upload with presigned URLs"""
    s3_client = boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    try:
        response = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
        upload_id = response["UploadId"]
        print(f"Created multipart upload with ID: {upload_id}")

        parts = []
        part_number = 1

        with open(file_path, "rb") as f:
            while True:
                # Read chunk
                chunk = f.read(chunk_size)
                if not chunk:
                    break

                print(f"Uploading part {part_number}...")

                # Generate presigned URL for this part
                presigned_url = s3_client.generate_presigned_url(
                    "upload_part",
                    Params={
                        "Bucket": bucket_name,
                        "Key": object_key,
                        "PartNumber": part_number,
                        "UploadId": upload_id,
                    },
                    ExpiresIn=3600,  # URL expires in 1 hour
                )

                # Upload the part using the presigned URL
                response = requests.put(presigned_url, data=chunk, timeout=120)
                response.raise_for_status()

                # Store part information
                parts.append(
                    {"ETag": response.headers["ETag"], "PartNumber": part_number}
                )
                part_number += 1

        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        print(f"Successfully uploaded {file_path} to s3://{bucket_name}/{object_key}")

    except ClientError as e:
        print("An error occurred:", e)
        s3_client.abort_multipart_upload(
            Bucket=bucket_name, Key=object_key, UploadId=upload_id
        )


if __name__ == "__main__":
    # Configuration

    test_cases = [
        (1, 1),
        (10, 10),
        (100, 10),
    ]
    for file_size_mb, part_size_mb in test_cases:
        file_path = generate_test_file(file_size_mb)
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"File {file_path} not found!")
            sys.exit(1)

        # Upload file
        try:
            print(f"Uploading {file_size_mb}MB file with part size {part_size_mb}MB...")
            multipart_upload_with_presigned_urls(
                file_path=file_path,
                bucket_name=BUCKET_NAME,
                object_key=str(uuid.uuid4()),
                chunk_size=part_size_mb * 1024 * 1024,
            )
        except Exception as e:
            print(f"Upload failed: {e}")
