#!/usr/bin/env python3
"""
Script to check and fix OpenSearch permissions
"""

import os
import sys
import json
import logging
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Get OpenSearch configuration
OPENSEARCH_ENDPOINT = os.getenv('OPENSEARCH_ENDPOINT')
OPENSEARCH_REGION = os.getenv('OPENSEARCH_REGION', os.getenv('AWS_REGION', 'us-east-1'))
OPENSEARCH_COLLECTION_NAME = os.getenv('OPENSEARCH_COLLECTION_NAME', 'tgresumeparser')

def get_current_identity():
    """Get the current AWS identity (IAM user or role)"""
    try:
        sts_client = boto3.client('sts')
        identity = sts_client.get_caller_identity()
        logger.info(f"Current AWS identity: {identity['Arn']}")
        return identity
    except Exception as e:
        logger.error(f"Error getting AWS identity: {str(e)}")
        return None

def check_collection_exists():
    """Check if the OpenSearch collection exists"""
    try:
        client = boto3.client('opensearchserverless', region_name=OPENSEARCH_REGION)
        response = client.list_collections()
        
        collections = response.get('collectionSummaries', [])
        collection_names = [col['name'] for col in collections]
        
        if OPENSEARCH_COLLECTION_NAME in collection_names:
            collection = next((c for c in collections if c['name'] == OPENSEARCH_COLLECTION_NAME), None)
            logger.info(f"Collection '{OPENSEARCH_COLLECTION_NAME}' exists with ID: {collection['id']}")
            return collection['id']
        else:
            logger.error(f"Collection '{OPENSEARCH_COLLECTION_NAME}' does not exist")
            return None
    except Exception as e:
        logger.error(f"Error checking collection: {str(e)}")
        return None

def list_access_policies():
    """List all OpenSearch access policies"""
    try:
        client = boto3.client('opensearchserverless', region_name=OPENSEARCH_REGION)
        response = client.list_access_policies(type='data')
        
        policies = response.get('accessPolicySummaries', [])
        logger.info(f"Found {len(policies)} data access policies")
        
        for policy in policies:
            logger.info(f"Policy: {policy['name']}, Type: {policy['type']}")
            
        return policies
    except Exception as e:
        logger.error(f"Error listing access policies: {str(e)}")
        return []

def create_or_update_access_policy(collection_id, identity_arn):
    """Create or update an access policy for the collection"""
    try:
        client = boto3.client('opensearchserverless', region_name=OPENSEARCH_REGION)
        
        # Define policy with all required permissions
        policy_name = f"{OPENSEARCH_COLLECTION_NAME}-access-policy"
        policy_document = {
            "Rules": [
                {
                    "Resource": [
                        f"aoss:collection/{collection_id}"
                    ],
                    "Permission": [
                        "aoss:CreateCollectionItems",
                        "aoss:DeleteCollectionItems",
                        "aoss:UpdateCollectionItems",
                        "aoss:DescribeCollectionItems"
                    ],
                    "Principal": [
                        identity_arn
                    ]
                },
                {
                    "Resource": [
                        f"aoss:index/{collection_id}/*"
                    ],
                    "Permission": [
                        "aoss:ReadDocument",
                        "aoss:WriteDocument",
                        "aoss:UpdateDocument",
                        "aoss:DeleteDocument"
                    ],
                    "Principal": [
                        identity_arn
                    ]
                }
            ]
        }
        
        # Check if policy exists
        existing_policies = list_access_policies()
        policy_exists = any(p['name'] == policy_name for p in existing_policies)
        
        if policy_exists:
            # Update existing policy
            logger.info(f"Updating existing access policy: {policy_name}")
            response = client.update_access_policy(
                name=policy_name,
                type='data',
                policy=json.dumps(policy_document)
            )
        else:
            # Create new policy
            logger.info(f"Creating new access policy: {policy_name}")
            response = client.create_access_policy(
                name=policy_name,
                type='data',
                policy=json.dumps(policy_document),
                description=f"Access policy for {OPENSEARCH_COLLECTION_NAME} collection"
            )
        
        logger.info(f"Access policy created/updated successfully: {response['accessPolicyDetail']['name']}")
        return True
    except Exception as e:
        logger.error(f"Error creating/updating access policy: {str(e)}")
        return False

def check_network_policy():
    """Check if a network policy exists for the collection"""
    try:
        client = boto3.client('opensearchserverless', region_name=OPENSEARCH_REGION)
        response = client.list_security_policies(type='network')
        
        policies = response.get('securityPolicySummaries', [])
        collection_policies = [p for p in policies if OPENSEARCH_COLLECTION_NAME in p['name']]
        
        if collection_policies:
            logger.info(f"Found network policy for collection: {collection_policies[0]['name']}")
            return True
        else:
            logger.warning(f"No network policy found for collection {OPENSEARCH_COLLECTION_NAME}")
            return False
    except Exception as e:
        logger.error(f"Error checking network policies: {str(e)}")
        return False

def create_network_policy(collection_id):
    """Create a network policy for the collection"""
    try:
        client = boto3.client('opensearchserverless', region_name=OPENSEARCH_REGION)
        
        policy_name = f"{OPENSEARCH_COLLECTION_NAME}-network-policy"
        policy_document = {
            "Rules": [
                {
                    "ResourceType": "collection",
                    "Resource": [
                        f"collection/{collection_id}"
                    ],
                    "SourceVPCEs": [],  # Empty means public access
                    "SourceIP": ["0.0.0.0/0"]  # Allow all IPs
                }
            ]
        }
        
        response = client.create_security_policy(
            name=policy_name,
            type='network',
            policy=json.dumps(policy_document),
            description=f"Network policy for {OPENSEARCH_COLLECTION_NAME} collection"
        )
        
        logger.info(f"Network policy created successfully: {response['securityPolicyDetail']['name']}")
        return True
    except ClientError as e:
        if 'ConflictException' in str(e) and 'already exists' in str(e):
            logger.info(f"Network policy already exists for {OPENSEARCH_COLLECTION_NAME}")
            return True
        logger.error(f"Error creating network policy: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error creating network policy: {str(e)}")
        return False

def main():
    """Main function to check and fix OpenSearch permissions"""
    logger.info("=== OpenSearch Permissions Fix Utility ===")
    
    # Get current identity
    identity = get_current_identity()
    if not identity:
        logger.error("Failed to get AWS identity. Check your credentials.")
        sys.exit(1)
    
    identity_arn = identity['Arn']
    
    # Check if collection exists
    collection_id = check_collection_exists()
    if not collection_id:
        logger.error("OpenSearch collection not found. Please create it first.")
        sys.exit(1)
    
    # List existing access policies
    list_access_policies()
    
    # Create or update access policy
    logger.info("Creating/updating access policy...")
    if create_or_update_access_policy(collection_id, identity_arn):
        logger.info("✅ Access policy updated successfully")
    else:
        logger.error("❌ Failed to update access policy")
    
    # Check network policy
    if not check_network_policy():
        logger.info("Creating network policy...")
        if create_network_policy(collection_id):
            logger.info("✅ Network policy created successfully")
        else:
            logger.error("❌ Failed to create network policy")
    
    logger.info("=== OpenSearch Permissions Fix Complete ===")
    logger.info("Wait a few minutes for the policies to take effect")

if __name__ == "__main__":
    main() 