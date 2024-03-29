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

"""Test upload process"""

import sys
from pathlib import Path

import pytest
from ghga_service_commons.utils.temp_files import big_temp_file  # type: ignore
from hexkit.providers.s3.testutils import (  # type: ignore
    config_from_localstack_container,
)
from pydantic import SecretStr
from testcontainers.localstack import LocalStackContainer  # type: ignore

from src.s3_upload_test import Config, async_main, objectstorage
from tests.fixtures.config import config_fixture  # noqa: F401

ALIAS = "test_file"
TEST_FILE_SIZE = 50 * 1024**2  # 50 MiB


@pytest.mark.asyncio
async def test_process(config_fixture: Config):  # noqa: F811
    """Test whole upload/download process for s3_upload script"""
    with LocalStackContainer(image="localstack/localstack:0.14.2").with_services(
        "s3"
    ) as localstack:
        s3_config = config_from_localstack_container(localstack)

        config = config_fixture.copy(
            update={
                "s3_endpoint_url": SecretStr(s3_config.s3_endpoint_url),
                "s3_access_key_id": SecretStr(s3_config.s3_access_key_id),
                "s3_secret_access_key": s3_config.s3_secret_access_key,
            }
        )
        storage = objectstorage(config=config)
        await storage.create_bucket(bucket_id=config.bucket_id)
        sys.set_int_max_str_digits(TEST_FILE_SIZE)
        with big_temp_file(TEST_FILE_SIZE) as file:
            await async_main(
                input_path=Path(file.name),
                config=config,
                verbose=True,
                debug=True,
            )
