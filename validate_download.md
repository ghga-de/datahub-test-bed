# Validate Download

The command tests the download functionality from the configured S3 or Ceph clusters using presigned URLs. It ensures that the download process works correctly and validates the integrity of the downloaded file.

## Purpose

- Ensure that files can be downloaded using presigned URLs.
- Validate the integrity of the downloaded file by comparing checksums.
- Report any issues encountered during the download process.

## Usage

`datahub-test-bed validate-download --config-path '/example/config.yaml' --object-key '<object_key>'`

If the `--object-key` is not provided, a test file will be created and used for the validation.

## Configuration

Create a YAML file with the following configuration. Replace the placeholders with the actual values from your cluster:

```yaml
s3_url_endpoint: "https://<URL>"
bucket: "<bucket_id>"
account_for_upload:
  name: "<account_name>"
  s3_access_key_id: "<key>"
  s3_secret_access_key: "<secret>"
account_for_download:
  name: "<account_name>"
  s3_access_key_id: "<key>"
  s3_secret_access_key: "<secret>"
output_dir: "<output_directory>"
```

The parameter `account_for_download` is the account used for creating the presigned URLs and download.

The parameter `account_for_upload` is the account used for uploading the test file.

Optional parameter:
- `test_file_size`: (integer), default 52428800 (50MB)
- `presigned_url_expiration`: (integer), default 60 seconds

## Interpreting the Results

The process logs the results to the standard output with different levels of verbosity. Please check all the logs at the end of the process for any error messages or explanations.
