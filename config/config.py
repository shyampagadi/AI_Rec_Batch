import os
import logging
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
AWS_PROFILE = os.getenv('AWS_PROFILE', 'default')

# S3 Configuration
S3_BUCKET_NAME = os.getenv('SOURCE_BUCKET')
S3_RAW_PREFIX = os.getenv('SOURCE_PREFIX', 'raw/')
S3_PROCESSED_PREFIX = os.getenv('DESTINATION_PREFIX', 'processed/')
S3_ERROR_PREFIX = os.getenv('ERROR_PREFIX', 'errors/')

# Local Storage Configuration
LOCAL_OUTPUT_DIR = os.getenv('LOCAL_OUTPUT_DIR', 'output')
LOCAL_ERROR_DIR = os.getenv('LOCAL_ERROR_DIR', 'errors')

# AWS Bedrock Configuration
BEDROCK_MODEL_ID = os.getenv('MODEL_ID')
BEDROCK_EMBEDDINGS_MODEL = os.getenv('BEDROCK_EMBEDDINGS_MODEL', 'amazon.titan-embed-text-v2:0')
# Maximum tokens to target for LLM input (prompt + content)
# Default to 8000 for safety, but can be adjusted based on model
BEDROCK_MAX_INPUT_TOKENS = int(os.getenv('BEDROCK_MAX_INPUT_TOKENS', '8000'))
# Character to token ratio (approx 4 chars per token)
BEDROCK_CHAR_PER_TOKEN = float(os.getenv('BEDROCK_CHAR_PER_TOKEN', '4.0'))

# PostgreSQL Configuration
ENABLE_POSTGRES = os.getenv('ENABLE_POSTGRES', 'true').lower() == 'true'
POSTGRES_HOST = os.getenv('DB_HOST', 'resume-parser-cluster-instance-1.cjs4u208eeey.us-east-1.rds.amazonaws.com')
POSTGRES_PORT = int(os.getenv('DB_PORT', 5432))
POSTGRES_DB = os.getenv('DB_NAME', 'resume_parser')
POSTGRES_USER = os.getenv('DB_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

# DynamoDB Configuration
ENABLE_DYNAMODB = os.getenv('ENABLE_DYNAMODB', 'true').lower() == 'true'
DYNAMODB_TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME', 'resumedata')
DYNAMODB_REGION = os.getenv('DYNAMODB_REGION', AWS_REGION)
DYNAMODB_ENDPOINT = os.getenv('DYNAMODB_ENDPOINT')
DYNAMODB_READ_CAPACITY_UNITS = int(os.getenv('DYNAMODB_READ_CAPACITY_UNITS', 10))
DYNAMODB_WRITE_CAPACITY_UNITS = int(os.getenv('DYNAMODB_WRITE_CAPACITY_UNITS', 5))

# OpenSearch Configuration
ENABLE_OPENSEARCH = os.getenv('ENABLE_OPENSEARCH', 'false').lower() == 'true'
OPENSEARCH_SERVERLESS = os.getenv('OPENSEARCH_SERVERLESS', 'false').lower() == 'true'
OPENSEARCH_ENDPOINT = os.getenv('OPENSEARCH_ENDPOINT')
OPENSEARCH_COLLECTION_NAME = os.getenv('OPENSEARCH_COLLECTION_NAME', 'tgresumeparser')
OPENSEARCH_INDEX = os.getenv('OPENSEARCH_INDEX', 'resume-embeddings')
OPENSEARCH_REGION = os.getenv('OPENSEARCH_REGION', AWS_REGION)
OPENSEARCH_USERNAME = os.getenv('OPENSEARCH_USERNAME')
OPENSEARCH_PASSWORD = os.getenv('OPENSEARCH_PASSWORD')

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG')
LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOG_FILE = os.getenv('LOG_FILE', 'resume_parser.log')

# Processing Configuration
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '8'))  # Derived from .env file
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '25'))  # Derived from .env file
RESUME_FILE_EXTENSIONS = os.getenv('RESUME_FILE_EXTENSIONS', 'pdf,docx,doc,txt,rtf').split(',')

# API Gateway URLS:

OPENSEARCH_GATEWAY_API_URL=os.getenv('OPENSEARCH_GATEWAY_API_URL')
OPENSEARCH_REST_API_KEY=os.getenv('OPENSEARCH_REST_API_KEY')
def get_aws_credentials():
    """Return AWS credentials for services to use"""
    # Only include AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    # Region will be handled separately to avoid duplication issues
    credentials = {}
    
    # Add credentials only if they are set
    if AWS_ACCESS_KEY_ID:
        credentials['aws_access_key_id'] = AWS_ACCESS_KEY_ID
    
    if AWS_SECRET_ACCESS_KEY:
        credentials['aws_secret_access_key'] = AWS_SECRET_ACCESS_KEY
        
    return credentials

def get_postgres_connection_string():
    """Return PostgreSQL connection string"""
    return f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def print_config():
    """Print configuration settings for debugging"""
    config_dict = {
        'AWS_REGION': AWS_REGION,
        'AWS_PROFILE': AWS_PROFILE,
        'BEDROCK_MODEL_ID (MODEL_ID)': BEDROCK_MODEL_ID,
        'BEDROCK_EMBEDDINGS_MODEL': BEDROCK_EMBEDDINGS_MODEL,
        'S3_BUCKET_NAME (SOURCE_BUCKET)': S3_BUCKET_NAME,
        'S3_RAW_PREFIX (SOURCE_PREFIX)': S3_RAW_PREFIX,
        'S3_PROCESSED_PREFIX (DESTINATION_PREFIX)': S3_PROCESSED_PREFIX,
        'LOCAL_OUTPUT_DIR': LOCAL_OUTPUT_DIR,
        'BATCH_SIZE': BATCH_SIZE,
        'MAX_WORKERS': MAX_WORKERS,
        'RESUME_FILE_EXTENSIONS': RESUME_FILE_EXTENSIONS,
        'LOG_LEVEL': LOG_LEVEL,
        'ENABLE_POSTGRES': ENABLE_POSTGRES,
        'POSTGRES_HOST (DB_HOST)': POSTGRES_HOST,
        'POSTGRES_PORT (DB_PORT)': POSTGRES_PORT,
        'POSTGRES_DB (DB_NAME)': POSTGRES_DB,
        'POSTGRES_USER (DB_USER)': POSTGRES_USER,
        'ENABLE_DYNAMODB': ENABLE_DYNAMODB,
        'DYNAMODB_TABLE_NAME': DYNAMODB_TABLE_NAME,
        'DYNAMODB_REGION': DYNAMODB_REGION,
        'DYNAMODB_ENDPOINT': DYNAMODB_ENDPOINT
    }
    
    print("\n=== Configuration Settings ===")
    for key, value in config_dict.items():
        print(f"{key}: {value}")
    print("============================\n") 