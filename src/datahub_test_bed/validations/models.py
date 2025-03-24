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

"""Configurations for the storage validations."""

import boto3
from pydantic import BaseModel


class S3AccountConfig(BaseModel):
    """Model for Storage Account configurations."""

    name: str
    s3_access_key_id: str
    s3_secret_access_key: str


class Buckets(BaseModel):
    """Model for storage buckets."""

    interrogation_bucket: str
    permanent_bucket: str
    outbox_bucket: str


class StorageAccounts(BaseModel):
    """Model for storage profiles."""

    master: S3AccountConfig
    ifrs: S3AccountConfig
    dcs: S3AccountConfig


class StorageConfig(BaseModel):
    """Model for storage configurations."""

    s3_url_endpoint: str
    buckets: Buckets
    accounts: StorageAccounts


class BaseBotoClient:
    """A base client for interacting with S3/Ceph storage."""

    def __init__(self, s3_url_endpoint: str, account: S3AccountConfig):
        self.account = account
        self.profile_name = account.name
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=s3_url_endpoint,
            aws_access_key_id=account.s3_access_key_id,
            aws_secret_access_key=account.s3_secret_access_key,
        )
