"""A config fixture"""


from typing import Generator

import pytest
from pydantic import SecretStr

from src.s3_readiness_test import Config


@pytest.fixture
def config_fixture() -> Generator[Config, None, None]:
    """Generate a test Config file."""

    yield Config(
        s3_endpoint_url=SecretStr("s3://test_url"),
        s3_access_key_id=SecretStr("test_access_key"),
        s3_secret_access_key=SecretStr("test_secret_key"),
        bucket_id="test_bucket",
    )
