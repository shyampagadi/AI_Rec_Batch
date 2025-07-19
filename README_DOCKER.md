# Docker Deployment Guide for Resume Parser

This guide covers how to containerize the resume parser for deployment on AWS.

## Building the Docker Image

The Dockerfile included in this directory configures a container with all necessary dependencies:

- Python 3.9 environment
- Java JRE for Tika
- Tesseract OCR for image processing
- All Python dependencies from requirements.txt
- Pre-configured Tika settings

To build the Docker image locally:

```bash
cd resume_parser_NEW_FINAL_with_docs
docker build -t resume-parser:latest -f Dockerfile ..
```

## Deploying to Amazon ECR

1. **Prerequisites:**
   - AWS CLI installed and configured
   - Proper permissions to push to ECR

2. **Using the provided ECR deployment script:**
   ```bash
   # Make the script executable
   chmod +x ecr_deploy.sh
   
   # Set required variables (or edit the script directly)
   export AWS_REGION=us-east-1
   export ECR_REPOSITORY_NAME=resume-parser
   export IMAGE_TAG=latest
   
   # Run the script
   ./ecr_deploy.sh
   ```

3. **Manual ECR deployment:**
   ```bash
   # Get AWS account ID
   aws_account=$(aws sts get-caller-identity --query Account --output text)
   
   # Create ECR repository if it doesn't exist
   aws ecr create-repository --repository-name resume-parser
   
   # Login to ECR
   aws ecr get-login-password | docker login --username AWS --password-stdin ${aws_account}.dkr.ecr.${AWS_REGION}.amazonaws.com
   
   # Tag image for ECR
   docker tag resume-parser:latest ${aws_account}.dkr.ecr.${AWS_REGION}.amazonaws.com/resume-parser:latest
   
   # Push to ECR
   docker push ${aws_account}.dkr.ecr.${AWS_REGION}.amazonaws.com/resume-parser:latest
   ```

## Running on AWS Batch

The Docker image is designed to work with AWS Batch. Here's a sample job definition:

```json
{
    "jobDefinitionName": "resume-parser-job",
    "type": "container",
    "containerProperties": {
        "image": "[YOUR-ACCOUNT-ID].dkr.ecr.[REGION].amazonaws.com/resume-parser:latest",
        "vcpus": 2,
        "memory": 4096,
        "command": [
            "python", 
            "parse_resume.py",
            "--batch",
            "--max_files", "100",
            "--upload_to_s3", "true"
        ],
        "environment": [
            {"name": "AWS_REGION", "value": "us-east-1"},
            {"name": "S3_BUCKET_NAME", "value": "your-resume-bucket"},
            {"name": "S3_RAW_PREFIX", "value": "raw/"},
            {"name": "S3_PROCESSED_PREFIX", "value": "processed/"},
            {"name": "ENABLE_POSTGRES", "value": "true"},
            {"name": "POSTGRES_HOST", "value": "your-postgres-host"},
            {"name": "ENABLE_OPENSEARCH", "value": "true"},
            {"name": "OPENSEARCH_ENDPOINT", "value": "your-opensearch-endpoint"},
            {"name": "BEDROCK_MODEL_ID", "value": "anthropic.claude-v2"}
        ]
    }
}
```

## Environment Variables

The container accepts the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| AWS_REGION | AWS Region | us-east-1 |
| AWS_ACCESS_KEY_ID | AWS Access Key | - |
| AWS_SECRET_ACCESS_KEY | AWS Secret Key | - |
| S3_BUCKET_NAME | S3 Bucket for resumes | - |
| S3_RAW_PREFIX | S3 prefix for raw resumes | raw/ |
| S3_PROCESSED_PREFIX | S3 prefix for processed results | processed/ |
| ENABLE_POSTGRES | Enable PostgreSQL storage | true |
| POSTGRES_HOST | PostgreSQL host | - |
| POSTGRES_PORT | PostgreSQL port | 5432 |
| POSTGRES_DB | PostgreSQL database name | - |
| POSTGRES_USER | PostgreSQL username | - |
| POSTGRES_PASSWORD | PostgreSQL password | - |
| ENABLE_DYNAMODB | Enable DynamoDB storage | true |
| DYNAMODB_TABLE_NAME | DynamoDB table name | resume-data |
| ENABLE_OPENSEARCH | Enable OpenSearch storage | true |
| OPENSEARCH_ENDPOINT | OpenSearch endpoint | - |
| OPENSEARCH_INDEX | OpenSearch index name | resume-embeddings |
| BEDROCK_MODEL_ID | AWS Bedrock model ID | anthropic.claude-v2 |

## Running Locally

To run the container locally:

```bash
docker run -it --rm \
  -e AWS_ACCESS_KEY_ID=your_access_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret_key \
  -e S3_BUCKET_NAME=your-bucket \
  -e ENABLE_POSTGRES=false \
  resume-parser:latest \
  python parse_resume.py --batch --max_files 10
``` 