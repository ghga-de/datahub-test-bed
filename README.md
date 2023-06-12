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

# Data Hub Testbed

## Overview
The data hub testbed can be used to evaluate the compliance of an S3-compatible object
storage with regard to the requirements for up and downloading files.

It provides key
metrics such as elapsed duration and average file part processing time. These metrics
are logged and displayed in the terminal. An important note is that the terms "upload"
and "download" refer to "Encryption and Upload" and "Download and Decryption", respectively.
Additionally, the script does not perform any kind of file cleanup or removal upon completion.
Please keep this in mind when running the script consecutively.

## Prerequisites

Before running this test, your data hub should fulfill the following requirements:

- An S3-compatible Object Storage is running and available via network.
- A DevOps engineer who is familiar with the S3 API and basic unix administration is
  available to assist with the execution of this test bed.
- An empty bucket has been created that can be used for this test.
- S3 credentials are available to access this bucket with read and write permissions.
- A UNIX machine with access to the Object Storage API is available with Python 3.9 and
  pip installed.


## Troubleshooting and reporting issues:

- Please first make sure that your configuration file is correct.
- Check that your S3 Object Storage is accessible and your credentials
   are valid for depositing files to the corresponding bucket. To do so, we recommend
   using a tool such as WinSCP or FileZilla.
- Check that the machine running the script can reach the S3 API by running e.g.:
  `curl <path to the s3 api>`
- Consult your local administrator of the S3 API.
- If you have ruled out the possibility that the failure happens due to your local
  setup, please open an issue in this repository.

## Installation

1. Git clone this repository and navigate with the terminal into the repository.
2. Create a python virtual environment and activate it (for instruction see
[here](https://realpython.com/python-virtual-environments-a-primer/)).
3. Install dependencies using pip: `pip install -r ./requirements.txt`
4. Validate that the script is ready: `./src/s3_upload_test.py`

## Configuration
Configuration parameters for this script are provided using a YAML file
(e.g., `config.yaml`). This file should include the following fields:

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
./src/s3_upload_test.py --input-path="path/to/file" --config-path="path/to/config_file" -v
```

### Command Line Arguments
**Required:**
- --input-path: Specifies the path to the file you want to upload.
- --config-path: Specifies the path to the file containing the required S3 configuration values.

**Optional:**
- --verbose, -v: Enables verbose mode, which displays logs for individual file parts.
- By default, these logs are hidden.


### Example:
For example, if the file to be uploaded is located at `/home/files/data.txt` and the
config file is located at `/home/files/config.yaml`, the command to run the script would be:

The exact command to run the script would be:
```bash
./src/s3_upload_test.py --input-path="/home/files/data.txt" --config-path="/home/files/config.yaml"
```

Add -v or --verbose at the end of the command if you wish to view logs for individual file parts:
```bash
./src/s3_upload_test.py --input-path="/home/files/data.txt" --config-path="/home/files/config.yaml" -v
```
