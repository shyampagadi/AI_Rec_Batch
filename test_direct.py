#!/usr/bin/env python
"""
Direct API Test

A minimal script to test the API with a very small job description to avoid timeout issues.
"""

import requests
import os
import json
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
api_url = os.environ.get("OPENSEARCH_GATEWAY_API_URL")
api_key = os.environ.get("OPENSEARCH_REST_API_KEY") or os.environ.get("API_KEY")

if not api_url:
    print("Error: OPENSEARCH_GATEWAY_API_URL not set")
    sys.exit(1)

# Ensure URL has /resumes at the end
if not api_url.endswith('/resumes'):
    if api_url.endswith('/'):
        api_url = f"{api_url}resumes"
    else:
        api_url = f"{api_url}/resumes"

print(f"API URL: {api_url}")
print(f"API key available: {'Yes' if api_key else 'No'}")

# Headers
headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

if api_key:
    headers['x-api-key'] = api_key
    print("Using API key for authentication")

# Use a very simple job description to minimize processing time
job_description = "Python developer"

print("\nTesting GET request with query parameter...")
response = requests.get(
    api_url,
    headers=headers,
    params={"job_description": job_description, "max_results": 3},
    timeout=30
)
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:200]}")

print("\nTesting POST request with body...")
response = requests.post(
    api_url,
    headers=headers,
    json={"job_description": job_description, "max_results": 3},
    timeout=30
)
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:200]}") 