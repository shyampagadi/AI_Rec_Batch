# Getting Started with the Resume Parser & Matching System

This guide provides step-by-step instructions for new users to get up and running with the Resume Parser and JD Matching System.

## System Overview

The Resume Parser & Matching System is a comprehensive solution that:

1. **Parses and processes resumes** from various file formats (PDF, DOCX, DOC, TXT)
2. **Extracts structured information** about candidates (skills, experience, education, etc.)
3. **Generates vector embeddings** to represent resume content semantically
4. **Stores parsed data** in multiple databases for efficient retrieval
5. **Matches job descriptions** against processed resumes using semantic search and reranking

## Prerequisites

Before you begin, ensure you have:

- Python 3.9+ installed
- AWS account with Bedrock and OpenSearch access
- Required credentials configured (AWS_PROFILE or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)
- Sufficient permissions to create and access S3 buckets, OpenSearch domains, and AWS Bedrock models
- PostgreSQL database instance (optional, for PII data storage)
- DynamoDB table (optional, for structured data storage)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd resume_parser_NEW_V7
   ```

2. Set up a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment variables by creating a `.env` file based on `.env-example`:
   ```
   # AWS Configuration
   AWS_REGION=us-east-1
   AWS_PROFILE=default
   
   # Bedrock Configuration
   BEDROCK_MODEL_ID=meta.llama3-70b-instruct-v1:0
   BEDROCK_EMBEDDINGS_MODEL=amazon.titan-embed-text-v2:0
   
   # S3 Configuration
   S3_BUCKET_NAME=your-bucket-name
   S3_RAW_PREFIX=raw/
   S3_PROCESSED_PREFIX=processed/
   
   # Database Configuration
   ENABLE_POSTGRES=True
   POSTGRES_HOST=your-db-host.amazonaws.com
   POSTGRES_PORT=5432
   POSTGRES_DB=resume_parser
   POSTGRES_USER=username
   POSTGRES_PASSWORD=password
   
   # OpenSearch Configuration
   ENABLE_OPENSEARCH=True
   OPENSEARCH_HOST=your-opensearch-domain.us-east-1.es.amazonaws.com
   
   # DynamoDB Configuration
   ENABLE_DYNAMODB=True
   DYNAMODB_TABLE_NAME=resumedata
   ```

## Basic Workflow

The system follows this basic workflow:

1. **Upload resumes** to the S3 bucket raw/ folder
2. **Parse resumes** using the parsing script (`parse_resume.py`)
3. **Create job descriptions** as text files
4. **Match job descriptions** with resumes using the matching script (`retrieve_jd_matches.py`)
5. **Review and analyze** matching results

## Quick Start

### 1. Parse Resumes

Run the resume parsing script to process resumes from the S3 bucket:

```bash
python parse_resume.py
```

This will:
- Download resumes from the S3 bucket's raw folder
- Extract text and structure from each resume
- Generate embeddings for vector search
- Store parsed data in PostgreSQL, DynamoDB, and OpenSearch
- Upload processed results to the S3 bucket's processed folder

### 2. Match Job Descriptions with Resumes

Create a job description file (e.g., `my_job_description.txt`) with the job requirements.

Run the matching script:

```bash
python retrieve_jd_matches.py --jd_file my_job_description.txt --method vector
```

Command-line options:
- `--jd_file`: Path to the job description file
- `--method`: Search method (`vector`, `text`, or `hybrid`)
- `--max`: Maximum number of results to return (default: 20)
- `--exp`: Required years of experience (default: 3.0)
- `--no-rerank`: Disable reranking (use pure vector similarity)

### 3. Review Results

Matching results will be saved to a JSON file in the `output` directory with a timestamp, for example: `output/job_matches_20250526_231134.json`.

The results include:
- Detailed job description analysis
- Extracted skills and requirements
- Ranked list of matching candidates
- Matching scores and skill comparisons
- Candidate contact information (if available)

## Next Steps

After getting familiar with the basic workflow:

- Learn how to [configure the system](../installation/configuration.md) for optimal performance
- Understand [how to interpret matching results](./understanding-results.md) in detail
- Explore [advanced usage options](./using-jd-matching.md) for the JD matching system
- Read about the [vector embedding framework](../technical-docs/vector-embedding-framework.md) to understand how matching works

## Troubleshooting

If you encounter any issues:

- Verify your AWS credentials and permissions
- Check the application logs (default location: `logs/` directory)
- Ensure your OpenSearch domain is properly configured
- Refer to the [troubleshooting guide](../troubleshooting/common-issues.md) for common issues 