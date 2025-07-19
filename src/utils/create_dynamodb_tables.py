#!/usr/bin/env python3
import os
import sys
import boto3
from botocore.exceptions import ClientError

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import configuration from aws_credentials.py first, then fall back to config.py
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
    # Set default values for capacity units
    DYNAMODB_READ_CAPACITY_UNITS = 10
    DYNAMODB_WRITE_CAPACITY_UNITS = 5
except ImportError:
    try:
        from config.config import (
            DYNAMODB_TABLE_NAME, 
            DYNAMODB_REGION, 
            DYNAMODB_ENDPOINT, 
            ENABLE_DYNAMODB, 
            DYNAMODB_READ_CAPACITY_UNITS, 
            DYNAMODB_WRITE_CAPACITY_UNITS,
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY
        )
        print("Using credentials from config.py")
    except ImportError as e:
        print(f"Error importing configuration: {e}")
        print("Make sure you're running this script from the project root directory.")
        sys.exit(1)

# Only proceed if explicitly enabled
if not ENABLE_DYNAMODB:
    print("DynamoDB table creation is disabled (ENABLE_DYNAMODB != true). Exiting.")
    sys.exit(0)

# Use the configuration values
TABLE_NAME = DYNAMODB_TABLE_NAME
REGION_NAME = DYNAMODB_REGION
ENDPOINT_URL = DYNAMODB_ENDPOINT
RCU = DYNAMODB_READ_CAPACITY_UNITS if 'DYNAMODB_READ_CAPACITY_UNITS' in locals() else 10
WCU = DYNAMODB_WRITE_CAPACITY_UNITS if 'DYNAMODB_WRITE_CAPACITY_UNITS' in locals() else 5

print(f"Using AWS credentials: {AWS_ACCESS_KEY_ID[:4]}...{AWS_ACCESS_KEY_ID[-4:]}")
print(f"Region: {REGION_NAME}")
print(f"Table: {TABLE_NAME}")
print(f"Endpoint: {ENDPOINT_URL}")

# Initialize client with explicit credentials
dynamodb = boto3.client(
    "dynamodb",
    region_name=REGION_NAME,
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def create_resume_table():
    try:
        # Check if table already exists
        dynamodb.describe_table(TableName=TABLE_NAME)
        print(f"Table '{TABLE_NAME}' already exists – checking for required indexes...")
        
        # Check if indexes exist and add them if needed
        try:
            table_description = dynamodb.describe_table(TableName=TABLE_NAME)
            existing_indexes = table_description.get('Table', {}).get('GlobalSecondaryIndexes', [])
            existing_index_names = [index.get('IndexName') for index in existing_indexes]
            
            # Check if we need to add email index
            if 'email-index' not in existing_index_names:
                print("Adding email-index...")
                dynamodb.update_table(
                    TableName=TABLE_NAME,
                    AttributeDefinitions=[
                        {"AttributeName": "email", "AttributeType": "S"}
                    ],
                    GlobalSecondaryIndexUpdates=[
                        {
                            "Create": {
                                "IndexName": "email-index",
                                "KeySchema": [
                                    {"AttributeName": "email", "KeyType": "HASH"}
                                ],
                                "Projection": {
                                    "ProjectionType": "ALL"
                                },
                                "ProvisionedThroughput": {
                                    "ReadCapacityUnits": RCU,
                                    "WriteCapacityUnits": WCU
                                }
                            }
                        }
                    ]
                )
                print("Added email-index to table")
                
            # Check if we need to add phone index
            if 'phone-index' not in existing_index_names:
                print("Adding phone-index...")
                dynamodb.update_table(
                    TableName=TABLE_NAME,
                    AttributeDefinitions=[
                        {"AttributeName": "phone_number", "AttributeType": "S"}
                    ],
                    GlobalSecondaryIndexUpdates=[
                        {
                            "Create": {
                                "IndexName": "phone-index",
                                "KeySchema": [
                                    {"AttributeName": "phone_number", "KeyType": "HASH"}
                                ],
                                "Projection": {
                                    "ProjectionType": "ALL"
                                },
                                "ProvisionedThroughput": {
                                    "ReadCapacityUnits": RCU,
                                    "WriteCapacityUnits": WCU
                                }
                            }
                        }
                    ]
                )
                print("Added phone-index to table")
                
        except Exception as e:
            print(f"Error updating indexes: {e}")
            
    except dynamodb.exceptions.ResourceNotFoundException:
        print(f"Creating table '{TABLE_NAME}'...")
        resp = dynamodb.create_table(
            TableName=TABLE_NAME,
            AttributeDefinitions=[
                # Define all attributes used in key schema and indexes
                {"AttributeName": "resume_id", "AttributeType": "S"},
                {"AttributeName": "email", "AttributeType": "S"},
                {"AttributeName": "phone_number", "AttributeType": "S"}
            ],
            KeySchema=[
                {"AttributeName": "resume_id", "KeyType": "HASH"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "email-index",
                    "KeySchema": [
                        {"AttributeName": "email", "KeyType": "HASH"}
                    ],
                    "Projection": {
                        "ProjectionType": "ALL"
                    },
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": RCU,
                        "WriteCapacityUnits": WCU
                    }
                },
                {
                    "IndexName": "phone-index",
                    "KeySchema": [
                        {"AttributeName": "phone_number", "KeyType": "HASH"}
                    ],
                    "Projection": {
                        "ProjectionType": "ALL"
                    },
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": RCU,
                        "WriteCapacityUnits": WCU
                    }
                }
            ],
            BillingMode="PROVISIONED",
            ProvisionedThroughput={
                "ReadCapacityUnits": RCU,
                "WriteCapacityUnits": WCU
            }
        )
        # Wait until the table is active
        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=TABLE_NAME)
        print(f"Table '{TABLE_NAME}' is now ACTIVE with required indexes.")

if __name__ == "__main__":
    try:
        create_resume_table()
    except ClientError as e:
        print("Error interacting with DynamoDB:", e)
        sys.exit(1)
