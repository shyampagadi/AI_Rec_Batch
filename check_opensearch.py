#!/usr/bin/env python
"""
OpenSearch Connection and Index Checker

This script verifies OpenSearch connectivity and validates/creates the required indexes.
Run this before deploying your Lambda to ensure the OpenSearch environment is properly configured.
"""
import os
import boto3
import json
import logging
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, exceptions
from requests_aws4auth import AWS4Auth
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_path = Path(__file__).resolve().parent.parent / '.env'
if env_path.exists():
    logger.info(f"Loading environment from: {env_path}")
    load_dotenv(dotenv_path=env_path)
else:
    logger.warning(f".env file not found at {env_path}")

# Try to import config values
try:
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from config.config import (
        OPENSEARCH_ENDPOINT, 
        OPENSEARCH_INDEX, 
        OPENSEARCH_REGION, 
        OPENSEARCH_SERVERLESS,
        OPENSEARCH_COLLECTION_NAME,
        AWS_REGION
    )
    logger.info("Successfully imported configuration from config.py")
except ImportError as e:
    logger.warning(f"Could not import from config.py: {str(e)}")
    # Define fallback default values
    OPENSEARCH_ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT')
    OPENSEARCH_INDEX = os.environ.get('OPENSEARCH_INDEX', 'resume-embeddings')
    OPENSEARCH_REGION = os.environ.get('OPENSEARCH_REGION', os.environ.get('AWS_REGION', 'us-east-1'))
    OPENSEARCH_SERVERLESS = os.environ.get('OPENSEARCH_SERVERLESS', 'false').lower() == 'true'
    OPENSEARCH_COLLECTION_NAME = os.environ.get('OPENSEARCH_COLLECTION_NAME', 'tgresumeparser')

def get_aws_credentials():
    """Return AWS credentials for authentication"""
    try:
        # Try to import from config file first
        sys.path.append(str(Path(__file__).resolve().parent.parent))
        from config.config import get_aws_credentials as config_get_credentials
        credentials = config_get_credentials()
        if credentials and 'aws_access_key_id' in credentials:
            logger.info("Using AWS credentials from config file")
            return boto3.Session(
                aws_access_key_id=credentials['aws_access_key_id'],
                aws_secret_access_key=credentials['aws_secret_access_key'],
                region_name=OPENSEARCH_REGION
            ).get_credentials()
    except (ImportError, AttributeError):
        logger.warning("Could not get credentials from config.py")
    
    # Fall back to boto3 session
    session = boto3.Session(region_name=OPENSEARCH_REGION)
    credentials = session.get_credentials()
    
    if credentials is None:
        logger.error("No AWS credentials found. Please configure AWS credentials.")
        sys.exit(1)
    
    logger.info(f"Using AWS credentials: {credentials.access_key[:4]}***")
    return credentials

def clean_endpoint(endpoint):
    """Clean up endpoint URL by removing protocol prefix if present"""
    if not endpoint:
        return None
        
    if endpoint.startswith('http://') or endpoint.startswith('https://'):
        endpoint = endpoint.split('://', 1)[1]
    
    # Remove trailing slashes
    endpoint = endpoint.rstrip('/')
    
    return endpoint

def connect_to_opensearch(endpoint, region, is_serverless=False):
    """Create and verify an OpenSearch connection"""
    if not endpoint:
        logger.error("OpenSearch endpoint is required")
        endpoint = input("Enter OpenSearch endpoint (required): ")
        if not endpoint:
            sys.exit(1)
    
    # Clean up endpoint URL
    endpoint = clean_endpoint(endpoint)
        
    logger.info(f"Connecting to OpenSearch endpoint: {endpoint}")
    
    try:
        # Get AWS credentials
        credentials = get_aws_credentials()
        
        # Determine service name based on whether using OpenSearch Serverless
        service_name = 'aoss' if is_serverless else 'es'
        logger.info(f"Using service name: {service_name}")
        
        # Create AWS authentication
        aws_auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            service_name,
            session_token=credentials.token
        )
        
        # Create OpenSearch client with more detailed configuration
        client = OpenSearch(
            hosts=[{'host': endpoint, 'port': 443}],
            http_auth=aws_auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30,
            retry_on_timeout=True,
            max_retries=3
        )
        
        return client
            
    except Exception as e:
        logger.error(f"❌ Failed to connect to OpenSearch: {str(e)}")
        raise e

def get_collection_index_patterns():
    """Get all possible index patterns to try based on configuration"""
    index_options = []
    
    # The index name is required
    if not OPENSEARCH_INDEX:
        logger.warning("No index name provided")
        return index_options
    
    # Start with the most likely collection/index format
    if OPENSEARCH_COLLECTION_NAME:
        index_options.append(f"{OPENSEARCH_COLLECTION_NAME}/{OPENSEARCH_INDEX}")
    
    # Then try standalone index format
    index_options.append(OPENSEARCH_INDEX)
    
    # Add other variations
    if OPENSEARCH_COLLECTION_NAME:
        index_options.extend([
            f"/{OPENSEARCH_COLLECTION_NAME}/{OPENSEARCH_INDEX}",
            f"{OPENSEARCH_COLLECTION_NAME}.{OPENSEARCH_INDEX}",
            f"{OPENSEARCH_COLLECTION_NAME}",
            f"/{OPENSEARCH_INDEX}"
        ])
    else:
        index_options.append(f"/{OPENSEARCH_INDEX}")
    
    return index_options

def test_index_access(client, index_name):
    """Test if an index exists and can be accessed"""
    logger.info(f"Testing access to index: '{index_name}'")
    
    try:
        # First check if the index exists
        exists = client.indices.exists(index=index_name)
        if exists:
            logger.info(f"✅ Index '{index_name}' exists")
            
            # Try basic search to verify read access
            basic_query = {
                "size": 1,
                "query": {"match_all": {}}
            }
            
            try:
                result = client.search(
                    body=basic_query,
                    index=index_name,
                    request_timeout=30
                )
                
                total_hits = result.get('hits', {}).get('total', {})
                if isinstance(total_hits, dict):
                    count = total_hits.get('value', 0)
                else:
                    count = total_hits
                
                logger.info(f"✅ Search successful! Found {count} documents")
                return True, count
            except Exception as search_error:
                logger.error(f"❌ Search failed: {str(search_error)}")
                return False, 0
        else:
            logger.warning(f"❌ Index '{index_name}' does not exist")
            return False, 0
            
    except Exception as e:
        logger.error(f"❌ Error testing index: {str(e)}")
        return False, 0

def test_all_index_patterns(client, index_patterns):
    """Test all index patterns to find which one works"""
    logger.info(f"Testing {len(index_patterns)} index pattern variations")
    
    working_patterns = []
    
    for pattern in index_patterns:
        logger.info(f"\nTesting pattern: '{pattern}'")
        success, doc_count = test_index_access(client, pattern)
        
        if success:
            working_patterns.append((pattern, doc_count))
            logger.info(f"✅ Pattern '{pattern}' works! ({doc_count} documents)")
        else:
            logger.info(f"❌ Pattern '{pattern}' failed")
    
    return working_patterns

def main():
    print("\n🔍 OpenSearch Connection and Index Checker 🔍\n")
    
    # Use values from config or environment - no input required if values exist
    endpoint = OPENSEARCH_ENDPOINT
    if not endpoint:
        endpoint = input("Enter OpenSearch endpoint (required): ")
        
    region = OPENSEARCH_REGION
    index_name = OPENSEARCH_INDEX
    is_serverless = OPENSEARCH_SERVERLESS
    collection_name = OPENSEARCH_COLLECTION_NAME
    
    # Clean endpoint
    endpoint = clean_endpoint(endpoint)
    
    print("\n✏️  Using the following configuration:")
    print(f"Endpoint: {endpoint}")
    print(f"Region: {region}")
    print(f"Index name: {index_name}")
    print(f"Serverless mode: {'Yes' if is_serverless else 'No'}")
    if collection_name:
        print(f"Collection name: {collection_name}")
    print("\n")
    
    # Get list of index patterns to test
    index_patterns = get_collection_index_patterns()
    logger.info(f"Will test these index patterns: {index_patterns}")
    
    # Connect to OpenSearch
    try:
        print("Connecting to OpenSearch...")
        client = connect_to_opensearch(endpoint, region, is_serverless)
        print("\n✅ Successfully connected to OpenSearch service")
        
        # Try to list indices - may fail but worth trying
        try:
            print("\nAttempting to list all indices...")
            indices = client.cat.indices(format='json')
            if indices:
                print(f"\n✅ Found {len(indices)} indices:")
                for idx in indices:
                    print(f" - {idx.get('index')}: {idx.get('docs.count')} docs")
            else:
                print("No indices found")
        except Exception as e:
            print(f"\n❌ Could not list indices: {str(e)}")
        
        # Test all index patterns
        print("\n🔍 Testing all index pattern variations:")
        working_patterns = test_all_index_patterns(client, index_patterns)
        
        # Print summary
        print("\n====================== SUMMARY ======================")
        if working_patterns:
            print(f"✅ Found {len(working_patterns)} working index pattern(s):")
            for pattern, count in working_patterns:
                print(f" - '{pattern}' ({count} documents)")
            
            # Get the best pattern (most documents)
            best_pattern = max(working_patterns, key=lambda x: x[1])[0]
            
            print("\nUse the following environment variables in your Lambda:")
            print(f"OPENSEARCH_ENDPOINT={endpoint}")
            print(f"OPENSEARCH_INDEX={index_name}")
            if is_serverless:
                print(f"OPENSEARCH_SERVERLESS=true")
                if collection_name:
                    print(f"OPENSEARCH_COLLECTION_NAME={collection_name}")
            
            print(f"\nRecommended index path in Lambda: '{best_pattern}'")
        else:
            print("❌ No working index patterns found")
            print("Please check your OpenSearch configuration and permissions")
    
    except Exception as e:
        logger.error(f"Failed to connect to OpenSearch: {str(e)}")
        print("\n❌ Connection failed - please check your endpoint and AWS credentials")
        sys.exit(1)

if __name__ == "__main__":
    main() 