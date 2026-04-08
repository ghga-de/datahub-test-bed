# S3 Bucket and Access Policy Setup for Federated Data Operations

This document provides the required S3 (or compatible) bucket and access policy setup for Data Hubs integrating with GHGA Central. It covers the bucket structure, dedicated service users, and the minimum bucket policies needed to support federated data operations across all environments.

## Terminology

**ACL (Access Control List)**: A mechanism that defines which users or accounts have what level of access on a bucket or object. In our context, the default ACL on a bucket is applied when it is created, granting the owner full permissions. ACLs are broader and distinct from bucket policies and do not offer the same granular and detailed control as bucket policies.

**Bucket Policy**: A JSON-based policy that provides fine-grained access control on buckets and objects. Unlike ACLs, bucket policies can specify more detailed permissions for individual accounts or sub-users. In our setup, bucket policies are used to restrict the actions available to service accounts.

**Account vs. Sub-User**: In Ceph, an account represents an independent entity. For example, the Master, IFRS, and DCS are individual accounts. However, sub-users are entities created under a root account and inherit certain permissions from the root unless explicitly restricted. For example, ghga-main is a root account, and the Master, IFRS, and DCS are individual sub-users.

**Copy Operations**: GHGA services use the Multipart Upload API when copying large files. This operation requires a different set of permissions (e.g., ListMultipartUploadParts, etc.). In this document, the term copy by default refers to the multipart copy operation.

## Environments

To facilitate management of the GHGA software lifecycle and maintain the integrity of the archived data, the buckets should be grouped in three separate environments to support software rollout on behalf of GHGA Central:
- **Testing** environment to test a scheduled updates
- **Staging** environment for final checkout before go-live
- **Production** environment for live operations.

If the storage system supports S3 namespaces, it's is recommended to leverage it for implementation of the required environments. Since users are bound to namespaces, each user will receive separate credentials per namespace.

## Buckets

The GHGA infrastructure requires four local S3 buckets at each Data Hub to support federated operations:
- **Inbox** to receive submitted datasets uploaded using the GHGA client.
- **Staging** (or "interrogation") to temporarily store submissions during decryption, validation and re-encryption.
- **Archive** (or "permanent") to permanently store re-encrypted datasets.
- **Outbox** to temporarily store requested datasets for download.

<div style="page-break-before: always;"></div>

## Users

The GHGA infrastructure is federated, with the micro services hosted at GHGA Central (DKFZ) requiring remote access to the local S3 buckets at each data hub. These services require the following dedicated S3 users:

- **Master**: The main local S3 user responsible for bucket maintenance; this may double as the Data Steward user.
- **IFRS**: GHGA Central’s Internal File Registry Service to maintain a database of submitted datasets along with their hosting Data Hubs and S3 URIs.
- **DCS**: GHGA Central’s Download Controller Service to access requested datasets by assigning pre-signed URLs.
- **UCS**: GHGA Central’s Upload Controller Service to manage file uploads by assigning pre-signed URLs.
- **DHFS**: The Data Hub File Service enabling file inspection and re-encryption at Data Hubs.

These users should not have access to any other bucket that is not intended, _see Bucket Policies section_.

### Sub-Users Approach

To isolate ownership and avoid permission conflicts, we're recommending the sub-user approach. Instead of relying on bucket policies and ACLs under the same root user, we suggest separating the roles into dedicated admin user and designated sub-users. _Please read the Terminology section at the bottom for definitions._

**References**
- User Accounts: <https://docs.ceph.com/en/latest/radosgw/account/>
- Admin Guide: <https://docs.ceph.com/en/reef/radosgw/admin/>

### Bucket Ownership

Creating buckets with a non-GHGA account, such as a data hub administrator, creates the bucket with the owner has default `FULL_CONTROL` ACL. To prevent any inheritance, GHGA root account or service users should not own the buckets. This is to avoid unwanted behavior caused by overlapping ownerships and inherited permissions. Therefore, bucket policies should be set only for GHGA root and it's sub-users for the required actions.

## Recommended User Setup

**GHGA Root Account**
The root _account_ for GHGA is `ghga-main`. All the sub-users belong to this account. It's different from the data hub admin account and doesn't own the buckets. It doesn't have any permissions by default.

**Sub-Users**
These are sub-users belong to the GHGA root account, as the root doesn't have any permissions to start with, neither the sub-users. Therefore bucket policies can be explicitly defined for each user.

- `ghga-main:master` (designated as the master sub-user)
- `ghga-main:ifrs` (service account)
- `ghga-main:dcs` (service account)
- `ghga-main:ucs` (service account)
- `ghga-main:dhfs` (service account)

<div style="page-break-before: always;"></div>

## Bucket Policies

Access to local S3 resources at each Data Hub must be restricted to the minimum required for federated operations. Bucket policies offer fine-grained access control per user to limit specific operations on the bucket. The bucket policies required are summarized in the table below.

| Bucket Name | Account          | Permissions                                                                                                                                                                        | Alternative Bucket Name |
| ----------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| inbox       | ghga-main:dhfs   | s3:ListBucket<br>s3:GetObject                                                                                                                                                      |                         |
| inbox       | ghga-main:ifrs   | s3:ListBucket<br>s3:GetObject<br>s3:DeleteObject                                                                                                                                   |                         |
| inbox       | ghga-main:ucs    | s3:ListBucket<br>s3:GetObject<br>s3:DeleteObject<br>s3:PutObject<br>s3:AbortMultipartUpload<br>s3:ListAllMyBuckets<br>s3:ListBucketMultipartUploads<br>s3:ListMultipartUploadParts |                         |
| inbox       | ghga-main:master | Full access (s3:_)                                                                                                                                                                 |                         |
| staging     | ghga-main:dhfs   | s3:ListBucket<br>s3:GetObject<br>s3:DeleteObject<br>s3:PutObject<br>s3:AbortMultipartUpload<br>s3:ListAllMyBuckets<br>s3:ListBucketMultipartUploads<br>s3:ListMultipartUploadParts | interrogation           |
| staging     | ghga-main:ifrs   | s3:GetObject<br>s3:DeleteObject<br>s3:ListBucket<br>s3:ListAllMyBuckets<br>s3:ListMultipartUploadParts                                                                             | interrogation           |
| staging     | ghga-main:master | Full access (s3:_)                                                                                                                                                                 | interrogation           |
| outbox      | ghga-main:dcs    | s3:ListBucket<br>s3:GetObject<br>s3:DeleteObject                                                                                                                                   |                         |
| outbox      | ghga-main:ifrs   | s3:ListBucket<br>s3:GetObject<br>s3:DeleteObject<br>s3:PutObject<br>s3:AbortMultipartUpload<br>s3:ListAllMyBuckets<br>s3:ListBucketMultipartUploads<br>s3:ListMultipartUploadParts |                         |
| outbox      | ghga-main:master | Full access (s3:_)                                                                                                                                                                 |                         |
| archive     | ghga-main:ifrs   | s3:ListBucket<br>s3:GetObject<br>s3:DeleteObject<br>s3:PutObject<br>s3:AbortMultipartUpload<br>s3:ListAllMyBuckets<br>s3:ListBucketMultipartUploads<br>s3:ListMultipartUploadParts | permanent               |
| archive     | ghga-main:master | Full access (s3:_)                                                                                                                                                                 | permanent               |

**Note**: Some storage systems (e.g. DELL ECS) may grant implicit access outside of bucket policy evaluation. In such cases, an explicit `Deny` statement should be added for each user on buckets they are not intended to access.
