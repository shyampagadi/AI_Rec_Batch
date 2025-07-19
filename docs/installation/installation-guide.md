# Installation Guide

This document provides detailed instructions for installing the Resume Parser & Matching System.

## System Requirements

### Hardware Requirements
- **CPU**: 4+ cores recommended for processing multiple resumes
- **RAM**: 8GB minimum, 16GB+ recommended
- **Storage**: 1GB for code and dependencies, plus storage for resume files
- **Network**: Internet connection for AWS services

### Software Requirements
- **Operating System**: Windows 10/11, macOS 10.15+, or Linux (Ubuntu 20.04+)
- **Python**: 3.9 or higher
- **AWS CLI**: Version 2.x configured with appropriate credentials
- **Git**: For cloning the repository

### AWS Requirements
- **AWS Account** with access to:
  - AWS Bedrock
  - Amazon OpenSearch Service
  - Amazon S3
  - Amazon DynamoDB (optional)
- **IAM Permissions** for:
  - `bedrock:InvokeModel`
  - `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`
  - `es:ESHttpGet`, `es:ESHttpPost`, `es:ESHttpPut`
  - `dynamodb:PutItem`, `dynamodb:GetItem`, `dynamodb:Query` (if using DynamoDB)

## Step-by-Step Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/resume-parser.git
cd resume-parser
```

### 2. Set Up Python Virtual Environment

#### On Windows
```bash
python -m venv venv
venv\Scripts\activate
```

#### On macOS/Linux
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

The main dependencies include:
- boto3
- opensearch-py
- python-dotenv
- pandas
- numpy
- spacy
- transformers
- pdf2image
- pytesseract
- pymupdf
- docx2txt

### 4. Configure AWS Credentials

If you haven't already configured AWS credentials, you can do so using the AWS CLI:

```bash
aws configure
```

Or create/update your `~/.aws/credentials` file:

```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
region = us-east-1
```

### 5. Set Up AWS Resources

#### 5.1. Create an S3 Bucket

Create an S3 bucket to store raw and processed resumes:

```bash
aws s3api create-bucket --bucket your-resume-bucket-name --region us-east-1
```

Create the necessary folders in the bucket:

```bash
aws s3api put-object --bucket your-resume-bucket-name --key raw/
aws s3api put-object --bucket your-resume-bucket-name --key processed/
```

#### 5.2. Create OpenSearch Domain

You can create an OpenSearch domain through the AWS Console or using CloudFormation. For the simplest setup:

1. Open AWS Console
2. Navigate to Amazon OpenSearch Service
3. Click "Create domain"
4. Choose a domain name (e.g., `resume-search`)
5. Select deployment type (Development for testing, Production with replicas for real use)
6. Choose the latest OpenSearch version (2.x or higher)
7. For network configuration, select "Public access" or "VPC" based on your security requirements
8. Configure access policies to allow your IAM user/role
9. Review and create the domain

#### 5.3. Create DynamoDB Table (Optional)

If using DynamoDB for structured data storage:

```bash
aws dynamodb create-table \
    --table-name resumedata \
    --attribute-definitions \
        AttributeName=resume_id,AttributeType=S \
    --key-schema \
        AttributeName=resume_id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --region us-east-1
```

### 6. Configure Environment Variables

Create a `.env` file in the project root:

```bash
cp .env-example .env
```

Edit the `.env` file with your specific configuration:

```ini
# AWS Configuration
AWS_REGION=us-east-1
AWS_PROFILE=default

# Bedrock Configuration
BEDROCK_MODEL_ID=meta.llama3-70b-instruct-v1:0
BEDROCK_EMBEDDINGS_MODEL=amazon.titan-embed-text-v2:0

# S3 Configuration
S3_BUCKET_NAME=your-resume-bucket-name
S3_RAW_PREFIX=raw/
S3_PROCESSED_PREFIX=processed/

# Database Configuration
ENABLE_POSTGRES=False  # Set to True if using PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=resume_parser
POSTGRES_USER=postgres
POSTGRES_PASSWORD=yourpassword

# OpenSearch Configuration
ENABLE_OPENSEARCH=True
OPENSEARCH_HOST=your-domain.us-east-1.es.amazonaws.com
OPENSEARCH_INDEX_NAME=resumes
OPENSEARCH_USERNAME=  # Leave empty if using IAM auth
OPENSEARCH_PASSWORD=  # Leave empty if using IAM auth

# DynamoDB Configuration
ENABLE_DYNAMODB=True
DYNAMODB_TABLE_NAME=resumedata

# Logging Configuration
LOG_LEVEL=INFO
```

### 7. PostgreSQL Setup (Optional)

If using PostgreSQL for PII data storage:

#### 7.1. Install PostgreSQL

- **Windows**: Download and install from [PostgreSQL website](https://www.postgresql.org/download/windows/)
- **macOS**: `brew install postgresql`
- **Ubuntu**: `sudo apt install postgresql postgresql-contrib`

#### 7.2. Create Database and User

```bash
sudo -u postgres psql
```

In the PostgreSQL shell:

```sql
CREATE DATABASE resume_parser;
CREATE USER resume_user WITH ENCRYPTED PASSWORD 'yourpassword';
GRANT ALL PRIVILEGES ON DATABASE resume_parser TO resume_user;
\q
```

#### 7.3. Initialize Database Schema

Run the schema creation script:

```bash
python scripts/initialize_database.py
```

### 8. Install OCR Dependencies (For PDF Processing)

#### Windows
1. Download and install [Tesseract OCR](https://github.com/UB-Mannheim/tesseract/wiki)
2. Add Tesseract to your PATH environment variable

#### macOS
```bash
brew install tesseract
```

#### Ubuntu
```bash
sudo apt install tesseract-ocr libtesseract-dev
```

### 9. Install Spacy Language Model

```bash
python -m spacy download en_core_web_lg
```

### 10. Initialize OpenSearch Index

Run the index initialization script:

```bash
python scripts/initialize_opensearch.py
```

This creates the necessary index with appropriate mappings for vector search.

### 11. Verify Installation

Run the verification script to ensure all components are working correctly:

```bash
python scripts/verify_installation.py
```

The script checks:
- AWS credentials and permissions
- S3 bucket access
- OpenSearch connectivity
- Bedrock model access
- Database connectivity (if enabled)
- OCR functionality

### 12. Upload Test Resumes

To test with sample resumes:

```bash
# Copy sample resumes to S3
aws s3 cp samples/resumes/ s3://your-resume-bucket-name/raw/ --recursive
```

### 13. Run Initial Test

Process a test resume to verify the full pipeline:

```bash
python parse_resume.py --file samples/resumes/sample1.pdf
```

Match a test job description:

```bash
python retrieve_jd_matches.py --jd_file samples/job_descriptions/software_engineer.txt
```

## Troubleshooting

### Common Issues

#### AWS Credentials Not Found
```
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```
**Solution**: Ensure AWS credentials are properly configured using `aws configure` or the `.env` file.

#### Bedrock Access Issues
```
AccessDeniedException: An error occurred (AccessDeniedException) when calling the InvokeModel operation
```
**Solution**: Verify your IAM user/role has access to AWS Bedrock and the specified model.

#### OpenSearch Connection Failed
```
opensearchpy.exceptions.ConnectionError: ConnectionError
```
**Solution**: 
- Verify the OpenSearch domain endpoint in your `.env` file
- Check security group settings to ensure access from your IP
- Verify IAM permissions for OpenSearch access

#### OCR Failure
```
pytesseract.pytesseract.TesseractNotFoundError: tesseract is not installed or not in PATH
```
**Solution**: Ensure Tesseract OCR is installed and in your system PATH.

### Logging

The system uses detailed logging to help diagnose issues:

- Logs are stored in the `logs/` directory
- The default log level is INFO, change to DEBUG in `.env` for more details
- Check logs for specific component failures

## Next Steps

After successful installation:

1. Review the [Configuration Guide](./configuration.md) for detailed system tuning
2. Follow the [Getting Started Guide](../user-guides/getting-started.md) to begin using the system
3. Upload your resumes to the S3 bucket for processing

## Uninstallation

To remove the system:

1. Delete AWS resources:
   ```bash
   aws s3 rb s3://your-resume-bucket-name --force
   # Delete OpenSearch domain and DynamoDB table through console if needed
   ```

2. Remove local files:
   ```bash
   deactivate  # Exit virtual environment
   rm -rf resume-parser/  # Remove project directory
   ``` 