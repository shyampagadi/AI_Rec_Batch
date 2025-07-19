#!/bin/bash

# Resume Parser Batch Processing Script
# This script helps run the resume parser in a Docker container

set -e  # Exit on error

# Default values
MAX_FILES=100
UPLOAD_TO_S3=true
ENV_FILE=".env"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --max-files) MAX_FILES="$2"; shift ;;
        --no-upload) UPLOAD_TO_S3=false ;;
        --env-file) ENV_FILE="$2"; shift ;;
        --help) 
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --max-files N     Process up to N files (default: 100)"
            echo "  --no-upload       Don't upload results to S3"
            echo "  --env-file FILE   Use specific environment file (default: .env)"
            echo "  --help            Show this help message"
            exit 0
            ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Error: Docker Compose is not installed or not in PATH"
    exit 1
fi

# Check if environment file exists
if [ ! -f "$ENV_FILE" ]; then
    echo "Creating sample environment file: $ENV_FILE"
    cat > "$ENV_FILE" << EOL
# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1

# S3 Configuration
S3_BUCKET_NAME=your-resume-bucket
S3_RAW_PREFIX=raw/
S3_PROCESSED_PREFIX=processed/

# PostgreSQL Configuration
ENABLE_POSTGRES=true
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=resume_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# DynamoDB Configuration
ENABLE_DYNAMODB=true
DYNAMODB_TABLE_NAME=resume-data

# OpenSearch Configuration
ENABLE_OPENSEARCH=true
OPENSEARCH_ENDPOINT=your-opensearch-endpoint
OPENSEARCH_INDEX=resume-embeddings

# Bedrock Configuration
BEDROCK_MODEL_ID=anthropic.claude-v2

# Batch Processing
MAX_FILES=$MAX_FILES
UPLOAD_TO_S3=$UPLOAD_TO_S3
EOL
    echo "Please edit $ENV_FILE with your actual credentials and settings."
    exit 1
fi

# Export environment variables for docker-compose
export MAX_FILES
export UPLOAD_TO_S3

echo "Starting Resume Parser Batch Processing..."
echo "- Max files to process: $MAX_FILES"
echo "- Upload to S3: $UPLOAD_TO_S3"
echo "- Using environment file: $ENV_FILE"

# Create required directories if they don't exist
mkdir -p logs output temp

# Run the Docker container with the specified parameters
docker-compose --env-file "$ENV_FILE" -f docker-compose.yml up --build

echo "Resume parsing completed!" 