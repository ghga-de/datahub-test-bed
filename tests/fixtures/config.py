# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
#
"""A config fixture"""


from typing import Generator

import pytest
import yaml

from src.s3_upload_test import Config
from tests.fixtures.utils import BASE_DIR

TEST_CONFIG_YAML = BASE_DIR / "test_config.yaml"


@pytest.fixture
def config_fixture() -> Generator[Config, None, None]:
    """Read config from test_config.yaml"""

    with open(TEST_CONFIG_YAML, "r", encoding="utf-8") as config_file:
        test_config = yaml.safe_load(config_file)

    yield Config(**test_config)
