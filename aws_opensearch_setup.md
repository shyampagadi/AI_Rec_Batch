# OpenSearch Serverless Setup Guide

Based on our diagnostics, your OpenSearch Serverless collection exists and the index has been created correctly, but you're missing the required **access policies** to allow the application to properly read/write data.

## Required AWS Access Policies

You need to set up two types of policies for OpenSearch Serverless:

1. **Data Access Policy** - Controls who can read/write data to your collection
2. **Network Policy** - Controls which networks can connect to your collection

## 1. Create Data Access Policy

Follow these steps in the AWS Console:

1. Open the AWS Console and navigate to **Amazon OpenSearch Service**
2. In the left navigation, click **Serverless** 
3. Click the **Security** tab and select **Data access policies**
4. Click **Create data access policy**
5. Enter the following details:
   - **Name**: `tg-resume-parser-policy`
   - **Rules**: Add multiple rules as follows:

   **Rule 1: Index Management Access**
   - **Resource type**: Index
   - **Resource**: `tgresumeparser/*`
   - **Permission**: Select all (index, read, write, etc.)
   - **Principals**: Add your AWS account ID or IAM role/user ARNs

   **Rule 2: Collection Access**
   - **Resource type**: Collection
   - **Resource**: `tgresumeparser`
   - **Permission**: Select all permissions
   - **Principals**: Add your AWS account ID or IAM role/user ARNs

6. Click **Create**

## 2. Create Network Policy

1. In the OpenSearch Serverless section, click **Security** → **Network policies**
2. Click **Create network policy**
3. Enter the following details:
   - **Name**: `tg-resume-parser-network`
   - **Policy scope**: Choose **Rule-based access**
   - **Rules**: Add the following rules:

   **Rule 1: Public Access**
   - **Rule name**: `PublicAccess`
   - **Access type**: Public
   - **Collection**: Select your `tgresumeparser` collection
   - **Type**: Choose both **OpenSearch Dashboards** and **OpenSearch Endpoints**

4. Click **Create**

## 3. Verify Configuration

After creating both policies, you should:

1. Wait a few minutes for the policies to propagate
2. Run the check_opensearch.py script again to verify connections
3. If issues persist, check:
   - IAM permissions for your user/role
   - Region settings in your .env file
   - Network connectivity

## 4. Resume Testing

Once your policies are in place, update your .env file to re-enable OpenSearch:

```
ENABLE_OPENSEARCH=true
```

Then try running your resume parser again.

## Common Errors and Solutions

### 404 Not Found
- **Cause**: Missing data access policy or incorrect endpoint
- **Solution**: Create proper data access policies and verify endpoint

### 403 Forbidden
- **Cause**: IAM permissions issue
- **Solution**: Check your IAM role/user has proper permissions

### Connection Timeout
- **Cause**: Network policy not configured
- **Solution**: Create proper network policy allowing your IP/VPC 