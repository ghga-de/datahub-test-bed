# Validate Storage Permissions

The command tests common S3 operations on the configured S3 or Ceph clusters to verify that account setups, bucket policies, and account scopes meet the requirements of the GHGA services. It is designed specifically for validating these policies and is not intended for generalized use outside the GHGA scope.

## Purpose

- Ensure that scoped accounts in storage nodes are compatible with GHGA service requirements.
- Ensure cross-account permissions are correctly configured.
- Report any missing or unnecessary permissions.

## Usage

`datahub-test-bed validate-storage-permissions --config-path '/example/config.yaml'`


## Configuration

Create a YAML file with the following configuration. Replace the placeholders with the actual values from your cluster:

```yaml
s3_url_endpoint: "https://<URL>"
buckets:
  interrogation_bucket: "<bucket_id>"
  permanent_bucket: "<bucket_id>"
  outbox_bucket: "<bucket_id>"
accounts:
  master:
    name: "<account_name>"
    s3_access_key_id: "<key>"
    s3_secret_access_key: "<secret>"
  ifrs:
    name: "<account_name>"
    s3_access_key_id: "<key>"
    s3_secret_access_key: "<secret>"
  dcs:
    name: "<account_name>"
    s3_access_key_id: "<key>"
    s3_secret_access_key: "<secret>"
```

## Interpreting the Results

The process logs the results of each validation to the standard output with different level of verbosity. The script continues even when there are fails to provide the full overview of the validation.
Please check all the logs at the end of process for any error messages or explanation.

1. **INFO** logs indicate that a certain action works as expected.
2. **ERROR** logs indicate a potential problem that should not occur.

In the results there are two types of messages in each log level.

**INFO logs**

1. Regular success messages
2. Expected failure cases. These are logged at the INFO level because they are anticipated.
  - E.g. Expected fail on operation %s to %s bucket using %s. Reason: %s

**ERROR logs**

2. Regular caught errors or exceptions that indicate a problem, e.g., missing permissions
3. Cases where an expected failure does not occur.
  - E.g. Account %s should not be able to upload to bucket %s


## Anticipated Permissions per Account and Bucket

Below is an overview of the operations allowed and validated during the run. These operations are tested to confirm that permissions are correctly set, but they do not define detailed policies or ownership. Detailed guidelines will be addressed in a separate SOP.

| **Account** | **Bucket**             | **HeadBucket** | **ListObjects** | **MultipartUpload** | **MultipartCopy**           | **Delete** | **PresignedURL** |
|-------------|------------------------|----------------|-----------------|---------------------|-----------------------------|-----------|------------------|
| MASTER      | INTERROGATION_BUCKET  | Yes            | Yes               | Yes                 | -                           | Yes       | -                |
| MASTER      | PERMANENT_BUCKET      | Yes            | Yes               | -                   | -                           | Yes       | -                |
| MASTER      | OUTBOX_BUCKET         | Yes            | Yes               | -                   | -                           | Yes       | -                |
| IFRS        | INTERROGATION_BUCKET  | Yes            | Yes             | -                   | Interrogation → Permanent     | Yes       | -                |
| IFRS        | PERMANENT_BUCKET      | Yes            | Yes             | Yes                 | Permanent → Outbox            | Yes       | -                |
| IFRS        | OUTBOX_BUCKET         | Yes            | Yes             | Yes                 | Permanent → Outbox            | -         | -                |
| DCS         | OUTBOX_BUCKET         | Yes            | Yes             | -                   | -                           | Yes       | Yes             |
