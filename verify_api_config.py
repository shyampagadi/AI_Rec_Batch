#!/usr/bin/env python
"""
API Gateway Verification Script

This script attempts to verify the correct API Gateway configuration by trying
multiple endpoint formats and authentication methods.
"""

import requests
import os
import json
import sys
import time
from dotenv import load_dotenv

# Load environment variables from .env file
def load_environment():
    env_path = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_path):
        print(f"Loading environment from {env_path}")
        load_dotenv(env_path)
    else:
        print("No .env file found, using environment variables")

    # Get the API URL (base domain)
    api_url = os.environ.get("OPENSEARCH_GATEWAY_API_URL")
    if not api_url:
        print("Error: OPENSEARCH_GATEWAY_API_URL is not set")
        sys.exit(1)
    
    # Extract base domain (up to the stage part)
    # Handle URLs like https://p1w63vjfu7.execute-api.us-east-1.amazonaws.com/dev/resumes
    if api_url.startswith('http'):
        # Split by protocol
        parts = api_url.split('://', 1)
        protocol = parts[0]
        rest = parts[1] if len(parts) > 1 else ''
        
        # Split the rest by slashes
        path_parts = rest.split('/')
        
        # Find the domain and stage
        domain = path_parts[0] if path_parts else ''
        stage = path_parts[1] if len(path_parts) > 1 else ''
        
        # Reconstruct base domain with protocol, domain and stage
        base_domain = f"{protocol}://{domain}/{stage}" if stage else f"{protocol}://{domain}"
    else:
        base_domain = api_url
        
    # Get API key(s)
    api_key = os.environ.get("OPENSEARCH_REST_API_KEY") or os.environ.get("API_KEY")
    
    return base_domain, api_key

def test_endpoint(url, api_key=None, method="GET", data=None):
    """Test an API endpoint with optional authentication"""
    print(f"\n{'=' * 60}")
    print(f"Testing endpoint: {url}")
    print(f"Method: {method}")
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    if api_key:
        headers['x-api-key'] = api_key
        print("Using API key authentication")
    
    if data:
        print(f"Request body: {json.dumps(data)[:100]}...")
    
    start_time = time.time()
    
    try:
        if method.upper() == "POST":
            response = requests.post(url, headers=headers, json=data, timeout=10)
        else:
            response = requests.get(url, headers=headers, timeout=10)
        
        time_taken = (time.time() - start_time) * 1000  # in ms
        
        # Print response details
        print(f"Status code: {response.status_code}")
        print(f"Time taken: {time_taken:.2f}ms")
        
        if response.status_code == 200:
            print("SUCCESS! Endpoint working correctly")
            try:
                result = response.json()
                print(f"Response preview: {json.dumps(result)[:200]}...")
            except:
                print(f"Response content: {response.text[:200]}...")
        else:
            print(f"Error response: {response.text[:200]}")
            
        return response.status_code, response.text
    except Exception as e:
        print(f"Request failed with error: {str(e)}")
        return None, str(e)

def main():
    # Load environment
    base_domain, api_key = load_environment()
    print(f"Base domain: {base_domain}")
    print(f"API key available: {'Yes' if api_key else 'No'}")
    
    # Default test data
    test_data = {
        "job_description": "Senior Software Engineer with 5+ years experience in Python and AWS"
    }
    
    # List of potential endpoint formats to try
    endpoint_formats = [
        "",                         # Base domain only
        "/",                        # Base with trailing slash
        "/resume",                  # Singular resource
        "/resumes",                 # Plural resource
        "/resume/",
        "/resumes/",
        "/resume/matching",
        "/resumes/matching",
        "/resume/matching/",
        "/resumes/matching/"
    ]
    
    # Try all endpoints with GET and POST
    successful_endpoints = []
    for endpoint in endpoint_formats:
        full_url = f"{base_domain}{endpoint}"
        
        # Try GET
        status_code, _ = test_endpoint(full_url, api_key, method="GET")
        if status_code == 200:
            successful_endpoints.append((full_url, "GET"))
        
        # Try POST
        status_code, _ = test_endpoint(full_url, api_key, method="POST", data=test_data)
        if status_code == 200:
            successful_endpoints.append((full_url, "POST"))
    
    # Summary
    print("\n" + "=" * 60)
    print("TESTING COMPLETE")
    print("=" * 60)
    
    if successful_endpoints:
        print("WORKING ENDPOINTS:")
        for url, method in successful_endpoints:
            print(f"- {method} {url}")
            
        print("\nRECOMMENDED CONFIGURATION:")
        best_url, best_method = successful_endpoints[0]
        print(f"URL: {best_url}")
        print(f"Method: {best_method}")
        print(f"Add this to your .env file: OPENSEARCH_GATEWAY_API_URL={best_url}")
    else:
        print("NO WORKING ENDPOINTS FOUND!")
        print("Possible issues:")
        print("1. Incorrect API key - check that the API key is valid and associated with this API")
        print("2. API Gateway not configured correctly - check resource paths and methods")
        print("3. Lambda function not deployed or failing - check CloudWatch logs")
        print("4. CORS issues - check if the API has CORS enabled")
        print("\nRecommended actions:")
        print("1. Check the API configuration in AWS API Gateway console")
        print("2. Verify the API key in the API Gateway console")
        print("3. Check if the Lambda function is deployed and working")
        print("4. Check CloudWatch logs for Lambda execution errors")

if __name__ == "__main__":
    main() 