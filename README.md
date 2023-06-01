<!--
 Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
 for the German Human Genome-Phenome Archive (GHGA)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->

# S3 Upload Testbed

## Overview
The S3 Upload Testbed is a script designed to evaluate the performance during file upload and download operations with an S3-compatible storage service. It provides key metrics such as elapsed duration and average file part processing time. These metrics are logged and displayed in the terminal. An important note is that the terms "upload" and "download" refer to "Encryption and Upload" and "Download and Decryption", respectively.

## Prerequisites
Before running the script, you must prepare an S3 configuration YAML file (e.g., `config.yaml`). This file should include the following fields:

- `s3_endpoint_url`: The endpoint to be used for S3 API calls within the script.
- `s3_access_key_id`: The globally unique IAM user ID for the S3 instance.
- `s3_secret_access_key`: The secret access key associated with the above access key ID.
- `bucket_id`: The ID of the S3 bucket. Please note, this ID cannot contain underscores.
- `part_size`: The default file part size in MiB for multipart file transfer.

Here's a sample configuration that can be adapted for your needs:

```yaml
s3_endpoint_url: http://localstack:4566
s3_access_key_id: test_access_key
s3_secret_access_key: test_secret_key
bucket_id: test-bucket
part_size: 16
```

## Usage:
Once you have prepared the configuration file, you can run the script using the following command:
```bash
./s3_upload_test.py --input-path="path/to/file" --config-path="path/to/config_file" -v
```

### Command Line Arguments
**Required:**
- --input-path: Specifies the path to the file you want to upload.
- --config-path: Specifies the path to the file containing the required S3 configuration values.

**Optional:**
- --verbose, -v: Enables verbose mode, which displays logs for individual file parts. By default, these logs are hidden.


### Example:
For example, if the file to be uploaded is located at `/home/files/data.txt` and the config file is located at `/home/files/config.yaml`, the command to run the script would be:

The exact command to run the script would be:
```bash
./s3_upload_test.py --input-path="/home/files/data.txt" --config-path="/home/files/config.yaml"
```

Add -v or --verbose at the end of the command if you wish to view logs for individual file parts:
```bash
./s3_upload_test.py --input-path="/home/files/data.txt" --config-path="/home/files/config.yaml" -v
```
