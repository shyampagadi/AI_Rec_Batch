import os
import json
import logging
import boto3
import requests
import traceback
import sys
from pathlib import Path
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# OpenSearch Configuration
OPENSEARCH_INDEX = os.getenv('OPENSEARCH_INDEX', 'resume-embeddings')
OPENSEARCH_ENDPOINT = os.getenv('OPENSEARCH_ENDPOINT')
OPENSEARCH_REGION = os.getenv('OPENSEARCH_REGION') or os.getenv('AWS_REGION') or 'us-east-1'

# Print configuration for debugging
logger.info(f"OpenSearch Endpoint: {OPENSEARCH_ENDPOINT}")
logger.info(f"OpenSearch Region: {OPENSEARCH_REGION}")
logger.info(f"OpenSearch Index: {OPENSEARCH_INDEX}")

# Define file paths
CURRENT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE = CURRENT_DIR / 'opensearch_data.json'
HTTP_OUTPUT_FILE = CURRENT_DIR / 'opensearch_data_http.json'

logger.info(f"Will write output to: {OUTPUT_FILE}")

def check_aws_credentials():
    """Verify AWS credentials are valid and print information for debugging"""
    try:
        # Get credentials and print info
        session = boto3.Session()
        credentials = session.get_credentials()
        if not credentials:
            logger.error("No AWS credentials found")
            return False
            
        # Check if credentials are valid by making a simple STS call
        sts = boto3.client('sts', region_name=OPENSEARCH_REGION)
        identity = sts.get_caller_identity()
        
        logger.info(f"AWS Account ID: {identity['Account']}")
        logger.info(f"AWS Identity ARN: {identity['Arn']}")
        logger.info(f"AWS User ID: {identity['UserId']}")
        
        # Check if the token is about to expire
        if hasattr(credentials, 'expiry_time'):
            logger.info(f"Credentials expire at: {credentials.expiry_time}")
        
        return True
    except Exception as e:
        logger.error(f"Error checking AWS credentials: {str(e)}")
        return False

def get_opensearch_client():
    """Initialize and return an OpenSearch client."""
    if not OPENSEARCH_ENDPOINT:
        logger.error("OpenSearch endpoint not provided.")
        return None
        
    try:
        # Get credentials
        credentials = boto3.Session().get_credentials()
        if not credentials:
            logger.error("No AWS credentials found")
            return None
            
        # Create auth for OpenSearch Serverless
        aws_auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            OPENSEARCH_REGION,
            'aoss',  # Use 'es' for OpenSearch managed service, 'aoss' for serverless
            session_token=credentials.token
        )
        
        # Log auth info (without sensitive data)
        logger.info(f"AWS Auth created with region {OPENSEARCH_REGION} and service 'aoss'")
        
        # Create OpenSearch client
        client = OpenSearch(
            hosts=[{'host': OPENSEARCH_ENDPOINT, 'port': 443}],
            http_auth=aws_auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30
        )
        
        logger.info("OpenSearch client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Error initializing OpenSearch client: {str(e)}")
        traceback.print_exc()
        return None

def test_opensearch_with_direct_request():
    """Test OpenSearch connection using direct HTTP request with specific headers."""
    try:
        # Get credentials
        credentials = boto3.Session().get_credentials()
        if not credentials:
            logger.error("No AWS credentials found")
            return False
            
        # Create auth
        aws_auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            OPENSEARCH_REGION,
            'aoss',
            session_token=credentials.token
        )
        
        # Create headers with required tokens for OpenSearch Serverless
        headers = {"Content-Type": "application/json"}
        headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
        if credentials.token:
            headers["x-amz-security-token"] = credentials.token
        
        # Test simple endpoint first (/_cluster/health is often accessible)
        health_url = f"https://{OPENSEARCH_ENDPOINT}/_cluster/health"
        
        logger.info(f"Testing connection to {health_url}")
        logger.info(f"Headers: {headers}")
        
        response = requests.get(
            health_url,
            auth=aws_auth,
            headers=headers,
            timeout=30
        )
        
        logger.info(f"Health check status code: {response.status_code}")
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Health check response: {response.text}")
            return True
        else:
            logger.error(f"Health check failed: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error during direct request test: {str(e)}")
        traceback.print_exc()
        return False

def view_opensearch_data():
    """Retrieve and log the first 10 documents from the OpenSearch index, and save them to a JSON file."""
    logger.info("\n=== Starting OpenSearch Data Retrieval ===\n")
    
    # Check AWS credentials first
    if not check_aws_credentials():
        logger.error("AWS credentials check failed, cannot proceed")
        return
    
    # Test connection with direct request
    logger.info("Testing OpenSearch connection with direct HTTP request...")
    if not test_opensearch_with_direct_request():
        logger.warning("Direct connection test failed, but attempting with SDK anyway")
    
    # Try using the OpenSearch client
    client = get_opensearch_client()
    if not client:
        logger.error("Failed to initialize OpenSearch client")
        return
        
    # Direct HTTP approach (more control over headers and debugging)
    try:
        logger.info("Attempting to query data using direct HTTP request...")
        
        # Get credentials
        credentials = boto3.Session().get_credentials()
        aws_auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            OPENSEARCH_REGION,
            'aoss',
            session_token=credentials.token
        )
        
        # Set up headers with required security tokens
        headers = {"Content-Type": "application/json"}
        headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
        if credentials.token:
            headers["x-amz-security-token"] = credentials.token
            
        # Construct search URL and request body
        search_url = f"https://{OPENSEARCH_ENDPOINT}/{OPENSEARCH_INDEX}/_search"
        search_body = {
            "size": 10,
            "query": {"match_all": {}}
        }
        
        logger.info(f"Making direct HTTP request to {search_url}")
        http_response = requests.post(
            search_url,
            auth=aws_auth,
            headers=headers,
            json=search_body,
            timeout=30
        )
        
        # Log HTTP response for debugging
        logger.info(f"HTTP Status Code: {http_response.status_code}")
        
        if http_response.status_code >= 200 and http_response.status_code < 300:
            # Success!
            response_data = http_response.json()
            hits = response_data.get('hits', {}).get('hits', [])
            logger.info(f"Retrieved {len(hits)} documents from OpenSearch index {OPENSEARCH_INDEX}")
            
            # Save the data to both files
            with open(HTTP_OUTPUT_FILE, 'w') as f:
                json.dump(hits, f, indent=4)
            logger.info(f"Data saved to {HTTP_OUTPUT_FILE}")
            
            # Also save to the main file
            with open(OUTPUT_FILE, 'w') as f:
                json.dump(hits, f, indent=4)
            logger.info(f"Data also saved to {OUTPUT_FILE}")
            
            # Don't return here, let's also try the SDK approach for completeness
        else:
            logger.error(f"HTTP Error: {http_response.status_code}, Response: {http_response.text}")
    except Exception as e:
        logger.error(f"Error with direct HTTP request: {str(e)}")
        traceback.print_exc()
    
    # Try with OpenSearch SDK as fallback
    try:
        logger.info("\nAttempting to query data using OpenSearch SDK...")
        response = client.search(
            index=OPENSEARCH_INDEX,
            body={
                "size": 10,
                "query": {"match_all": {}}
            }
        )
        
        hits = response.get('hits', {}).get('hits', [])
        logger.info(f"Retrieved {len(hits)} documents from OpenSearch index {OPENSEARCH_INDEX}")
        
        # Print first document if available
        if hits:
            logger.info(f"First document: {json.dumps(hits[0], indent=2)}")
        
        # Save the data to a JSON file
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(hits, f, indent=4)
        logger.info(f"Data saved to {OUTPUT_FILE}")
        
    except Exception as e:
        logger.error(f"Error retrieving data with OpenSearch SDK: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    view_opensearch_data()