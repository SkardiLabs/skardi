# S3 Data Source Configuration

This guide shows how to securely configure S3 data sources with Skardi server.

## Security First Approach

**🔒 IMPORTANT**: For security reasons, AWS credentials cannot be stored in configuration files. The server will reject any configuration that includes credential fields like `aws_access_key_id` or `aws_secret_access_key`.

## Quick Start

### 1. Set Environment Variables

```bash
# Method 1: Direct credentials (development/testing)
export AWS_ACCESS_KEY_ID="your_access_key_id"
export AWS_SECRET_ACCESS_KEY="your_secret_access_key"
export AWS_SESSION_TOKEN="your_session_token"  # Optional for temporary credentials

# Method 2: Convert an AWS CLI profile into env vars (AWS_PROFILE alone does
# NOT work — the S3 client never reads ~/.aws/; see Authentication Methods)
eval "$(aws configure export-credentials --profile your_profile_name --format env)"
```

### 2. Configure S3 Data Sources

Create a context YAML file with only the AWS region:

```yaml
kind: context

metadata:
  name: example-context
  version: 1.0.0

spec:
  data_sources:
    - name: "sales_data"
      type: "parquet"
      location: "remote_s3"
      path: "s3://my-bucket/sales/2024/sales.parquet"
      description: "Sales data in S3"

    - name: "customer_events"
      type: "csv"
      location: "remote_s3"
      path: "s3://analytics-bucket/events/events.csv"
      options:
        has_header: true
        delimiter: ","
      description: "Customer events CSV in S3"
```

### 3. Run the Server

```bash
# Start server with S3-enabled context
skardi-server --pipeline pipeline.yaml --ctx s3_context.yaml
```

### 4. Query the Data

Once the server is running with the S3 context loaded, query the registered
sources by name with the `skardi` CLI (see [docs/cli.md](cli.md)) — the CLI
is a thin HTTP client, so no local S3 access or credentials are needed on the
client side, only on the server:

```bash
skardi query -e "SELECT * FROM sales_data LIMIT 10"
skardi query -e "SELECT * FROM customer_events LIMIT 10" --table
```

## Connectivity Verification

The server automatically:
- ✅ Tests S3 connectivity at startup
- ✅ Verifies AWS credentials are valid
- ✅ Checks S3 paths exist and are accessible
- ✅ Validates IAM permissions
- ❌ Fails fast with detailed error messages if issues are found

## Authentication Methods

> **⚠️ Environment variables are the only working method today.** The S3 client
> is built from environment variables alone and never reads `~/.aws/`, and the
> credential check requires `AWS_ACCESS_KEY_ID` (or `AWS_PROFILE`) to be present.
> Profiles, SSO, and IAM roles therefore need the conversion step shown below.

### 1. Environment Variables (Development/CI) — works
```bash
export AWS_REGION="us-east-1"   # or AWS_DEFAULT_REGION; required, no default
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_SESSION_TOKEN="..."  # only for temporary credentials
```

### 2. AWS CLI Profiles / SSO — export them first
Setting `AWS_PROFILE` alone passes the credential check but contributes **no
credentials to request signing**, so requests then fail with an S3 auth error.
Convert the profile into environment variables instead:

```bash
eval "$(aws configure export-credentials --profile myprofile --format env)"
export AWS_REGION="us-east-1"
```

The same applies to SSO / Identity Center profiles (`aws configure sso`) — log
in, then export.

### 3. IAM Roles (EC2 / ECS / Lambda / EKS) — not supported yet
The underlying `object_store` client can source credentials from instance
metadata, ECS task endpoints, and EKS web-identity tokens, but skardi's
credential check rejects those setups before they are reached: it requires
`AWS_ACCESS_KEY_ID` or `AWS_PROFILE`, neither of which is set under an instance
profile or IRSA. Assume the role explicitly and export the result:

```bash
CREDS=$(aws sts assume-role --role-arn "$ROLE_ARN" --role-session-name skardi)
export AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$CREDS")
export AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$CREDS")
export AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$CREDS")
export AWS_REGION="us-east-1"
```

Note that assumed-role credentials expire (1h by default), and the server reads
the environment at registration *and* on each scan — so a long-running server
needs re-exported credentials, or a restart, before they lapse.

## Required IAM Permissions

Your AWS credentials need these minimum permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:HeadObject"
      ],
      "Resource": "arn:aws:s3:::your-bucket-name/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::your-bucket-name"
    }
  ]
}
```

## Supported File Types

- **CSV**: `type: "csv", location: "remote_s3"`
- **Parquet**: `type: "parquet", location: "remote_s3"`
- **Lance**: `type: "lance", location: "remote_s3"`

## Error Messages

The server provides detailed error messages for common issues:

### Invalid Credentials
```
Missing required AWS configuration for S3 data source: 'my_data' - missing AWS_ACCESS_KEY_ID environment variable or AWS_PROFILE
```

### File Not Found
```
Data source file not found: my_data -> s3://bucket/missing-file.parquet
```

### Connectivity Issues
```
S3 connectivity test failed for region 'us-east-1': access denied. Please verify:
1. AWS credentials are valid (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
2. AWS region 'us-east-1' is correct
3. S3 path 's3://bucket/file.parquet' exists and is accessible
4. IAM permissions allow s3:GetObject and s3:HeadObject on the bucket/object
```

### Credentials in Config (Security Error)
```
AWS credentials ('aws_access_key_id') must not be stored in configuration files for security reasons.
Please use environment variables instead:
- Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
- Or use AWS_PROFILE to specify an AWS credentials profile
- Or use IAM roles/instance profiles on AWS infrastructure
```

## Examples

See `ctx_s3_examples.yaml` for complete configuration examples.

## Troubleshooting

1. **Check AWS credentials**: `aws sts get-caller-identity`
2. **Test S3 access**: `aws s3 ls s3://your-bucket/`
3. **Verify region**: Make sure the region matches your bucket's region
4. **Check IAM policies**: Ensure your credentials have the required S3 permissions
