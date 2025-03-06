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

"""Main module for the CLI."""

from pathlib import Path

import typer
import yaml

from datahub_test_bed.validations.download.main import run_download_validation
from datahub_test_bed.validations.models import DownloadConfig, StorageConfig
from datahub_test_bed.validations.storage.main import run_validations

cli = typer.Typer(no_args_is_help=True)


@cli.command()
def hello():
    """Command to say hello."""
    msg = "Hello?"
    typer.echo(msg)
    return msg


@cli.command()
def validate_storage_permissions(
    config_path: Path = typer.Option(
        "config.yaml", help="Path to the storage configuration YAML file."
    ),
):
    """Run storage validations against the configured environment."""
    try:
        with config_path.open("r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
        storage_config = StorageConfig(**config_data)
        typer.echo("Storage config has been loaded")
    except Exception as e:
        typer.echo(f"Error loading storage config: {e}", err=True)
        raise typer.Exit(code=1) from e

    # Run the storage validations
    run_validations(storage_config)


@cli.command()
def validate_download(
    config_path: Path = typer.Option(
        "config.yaml", help="Path to the download configuration YAML file."
    ),
    object_key: str = typer.Option(
        None,
        help="The object key to download. If not provided, a test file will be created.",
    ),
):
    """Download a file from the configured environment using presigned URL."""
    try:
        with config_path.open("r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
        download_config = DownloadConfig(**config_data)
        typer.echo("Storage config has been loaded")
    except Exception as e:
        typer.echo(f"Error loading storage config: {e}", err=True)
        raise typer.Exit(code=1) from e

    # Run the download validation
    run_download_validation(download_config, object_key)
