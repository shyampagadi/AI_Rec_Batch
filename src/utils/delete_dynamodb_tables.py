#!/usr/bin/env python3
"""
Script to delete the DynamoDB table named by DYNAMODB_TABLE_NAME in your .env.
It loads AWS and DynamoDB parameters via python-dotenv, then deletes and waits
for the table to be fully removed.
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError

# Import configuration from our credentials file
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
try:
    from aws_credentials import (
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_REGION,
        ENABLE_DYNAMODB,
        DYNAMODB_TABLE_NAME,
        DYNAMODB_REGION,
        DYNAMODB_ENDPOINT
    )
    print("Using credentials from aws_credentials.py")
except ImportError:
    try:
        from config.config import (
            DYNAMODB_TABLE_NAME,
            DYNAMODB_REGION,
            DYNAMODB_ENDPOINT,
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            ENABLE_DYNAMODB
        )
        print("Using credentials from config.py")
    except ImportError:
        print("ERROR: Could not import AWS credentials from either aws_credentials.py or config.py")
        sys.exit(1)

# Ensure deletion is explicitly enabled
if not ENABLE_DYNAMODB:
    print("DynamoDB deletion disabled (ENABLE_DYNAMODB != true). Exiting.")
    sys.exit(0)

print(f"Using AWS credentials: {AWS_ACCESS_KEY_ID[:4]}...{AWS_ACCESS_KEY_ID[-4:]}")
print(f"Region: {DYNAMODB_REGION}")
print(f"Table: {DYNAMODB_TABLE_NAME}")
print(f"Endpoint: {DYNAMODB_ENDPOINT}")

# Initialize DynamoDB client with explicit credentials
dynamodb = boto3.client(
    "dynamodb",
    region_name=DYNAMODB_REGION,
    endpoint_url=DYNAMODB_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def delete_resume_table():
    """
    Attempt to delete the table, handle common exceptions,
    and wait until deletion completes.
    """
    if not DYNAMODB_TABLE_NAME:
        print("DYNAMODB_TABLE_NAME is not set. Exiting.")
        sys.exit(1)
        
    try:
        print(f"Initiating deletion of table '{DYNAMODB_TABLE_NAME}'...")
        dynamodb.delete_table(TableName=DYNAMODB_TABLE_NAME)
    except dynamodb.exceptions.ResourceNotFoundException:
        print(f"Table '{DYNAMODB_TABLE_NAME}' does not exist; nothing to delete.")
        return
    except ClientError as err:
        # Handle cases like table in CREATING/UPDATING state → ResourceInUseException
        print(f"Failed to delete table: {err.response['Error']['Message']}")
        sys.exit(1)

    # Wait until table no longer exists
    waiter = dynamodb.get_waiter("table_not_exists")
    print(f"Waiting for table '{DYNAMODB_TABLE_NAME}' to be fully deleted...")
    try:
        waiter.wait(TableName=DYNAMODB_TABLE_NAME)
        print(f"Table '{DYNAMODB_TABLE_NAME}' has been deleted successfully.")
    except ClientError as err:
        print(f"Error while waiting for deletion: {err}")
        sys.exit(1)

if __name__ == "__main__":
    delete_resume_table()
