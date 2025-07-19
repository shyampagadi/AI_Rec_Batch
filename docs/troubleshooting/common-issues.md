# Common Issues and Troubleshooting

This guide provides solutions for common issues you may encounter when using the Resume Parser & Matching System.

## Installation Issues

### AWS Credentials Not Found

**Issue:**
```
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```

**Solution:**
1. Configure AWS credentials using the AWS CLI:
   ```bash
   aws configure
   ```

2. Or set environment variables:
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_REGION=us-east-1
   ```

3. Verify your credentials file:
   ```bash
   cat ~/.aws/credentials
   ```

4. Check that your `.env` file has the correct AWS profile configured

### Bedrock Model Access Denied

**Issue:**
```
AccessDeniedException: An error occurred (AccessDeniedException) when calling the InvokeModel operation
```

**Solution:**
1. Verify your IAM user/role has access to AWS Bedrock
2. Check that you have permissions for the specific model being used
3. Confirm the model is available in your selected AWS region
4. Try a different model by editing the `.env` file

### OpenSearch Connection Failed

**Issue:**
```
opensearchpy.exceptions.ConnectionError: ConnectionError
```

**Solution:**
1. Verify the OpenSearch domain endpoint in your `.env` file
2. Check security group settings to ensure access from your IP
3. Check network connectivity to the domain
4. Verify IAM permissions for OpenSearch access

### OCR/Tesseract Not Found

**Issue:**
```
pytesseract.pytesseract.TesseractNotFoundError: tesseract is not installed or not in PATH
```

**Solution:**
1. Install Tesseract OCR:
   - Windows: Download from [UB-Mannheim](https://github.com/UB-Mannheim/tesseract/wiki)
   - macOS: `brew install tesseract`
   - Linux: `sudo apt install tesseract-ocr`

2. Add to PATH:
   - Windows: Add Tesseract installation directory to PATH
   - Linux/macOS: Verify installation with `which tesseract`

## Resume Processing Issues

### PDFs Not Processing Correctly

**Issue:** Some PDFs fail to extract text properly or result in poor-quality extractions.

**Solution:**
1. Check if the PDF is image-based or has security settings:
   ```bash
   pdfinfo document.pdf
   ```

2. Try pre-processing with OCR tools:
   ```bash
   python scripts/enhance_pdf.py --input document.pdf
   ```

3. Verify PDF is not password-protected

### Resume Parsing Errors

**Issue:** Error processing specific resume files or incorrect data extraction.

**Solution:**
1. Enable debug logging to see detailed parsing steps:
   ```
   LOG_LEVEL=DEBUG
   ```

2. Try processing with verbose output:
   ```bash
   python parse_resume.py --file resume.pdf --verbose
   ```

3. Check for unusual formatting in the resume
4. Try converting to a different format first (e.g., PDF to DOCX or TXT)

### S3 Upload/Download Errors

**Issue:**
```
botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the GetObject operation
```

**Solution:**
1. Check bucket permissions and policies
2. Verify the bucket exists in the specified region
3. Confirm your IAM user/role has s3:GetObject and s3:PutObject permissions
4. Check the bucket name in your `.env` file

## Job Description Matching Issues

### No Results Found

**Issue:** The system returns no results when matching a job description.

**Solution:**
1. Verify that resumes have been parsed and indexed:
   ```bash
   python scripts/list_indexed_resumes.py
   ```

2. Check if skills in the JD are too specific or uncommon:
   ```bash
   python scripts/analyze_jd.py --jd_file job_description.txt
   ```

3. Try reducing experience requirements:
   ```bash
   python retrieve_jd_matches.py --jd_file job_description.txt --exp 0
   ```

4. Try a different search method:
   ```bash
   python retrieve_jd_matches.py --jd_file job_description.txt --method text
   ```

### Poor Quality Matches

**Issue:** Matches don't seem relevant to the job description.

**Solution:**
1. Improve the job description with more specific skills and requirements
2. Try adjusting the reranking weights:
   ```bash
   python retrieve_jd_matches.py --jd_file job_description.txt --weights "vector=0.6,skill=0.3,experience=0.1,recency=0"
   ```

3. Use the hybrid search method for better results:
   ```bash
   python retrieve_jd_matches.py --jd_file job_description.txt --method hybrid
   ```

4. Check if the job description is clear and well-structured using the template format

### Search Performance Issues

**Issue:** Searches are slow or timeout.

**Solution:**
1. Reduce the maximum number of results:
   ```bash
   python retrieve_jd_matches.py --jd_file job_description.txt --max 10
   ```

2. Optimize the OpenSearch domain settings:
   - Increase instance size
   - Tune refresh interval
   - Configure dedicated master nodes

3. Check current OpenSearch cluster health:
   ```bash
   curl -X GET "https://your-domain.us-east-1.es.amazonaws.com/_cluster/health"
   ```

## Vector Embedding Issues

### Embedding Generation Errors

**Issue:**
```
Error generating embedding: Request exceeded maximum allowed payload size
```

**Solution:**
1. Check if the text being embedded is too large (>8000 tokens for most models)
2. Use chunking for large documents:
   ```bash
   python scripts/process_large_resume.py --file large_resume.pdf
   ```

3. Try a different embedding model in your `.env` file:
   ```
   BEDROCK_EMBEDDINGS_MODEL=amazon.titan-embed-text-v1:0
   ```

### Vector Search Returns Inconsistent Results

**Issue:** Vector search results vary or seem random.

**Solution:**
1. Check the embeddings model configuration
2. Verify the OpenSearch index mapping has the correct dimension
3. Try rebuilding the vector index:
   ```bash
   python scripts/rebuild_vector_index.py
   ```

4. Ensure the query text is properly preprocessed

## Database Issues

### PostgreSQL Connection Errors

**Issue:**
```
psycopg2.OperationalError: could not connect to server
```

**Solution:**
1. Verify PostgreSQL is running:
   ```bash
   systemctl status postgresql  # Linux
   brew services info postgresql  # macOS
   ```

2. Check connection details in `.env` file
3. Test connection directly:
   ```bash
   psql -h localhost -U username -d resume_parser
   ```

4. Check firewall settings if using a remote server

### DynamoDB Table Not Found

**Issue:**
```
botocore.exceptions.ClientError: An error occurred (ResourceNotFoundException)
```

**Solution:**
1. Verify the table exists:
   ```bash
   aws dynamodb describe-table --table-name resumedata
   ```

2. Check table name in `.env` file
3. Verify the table is in the same region as your AWS configuration
4. Create the table if it doesn't exist:
   ```bash
   python scripts/initialize_dynamodb.py
   ```

## Logging and Debugging

### Enabling Debug Logging

For more detailed logs:

1. Set LOG_LEVEL in `.env`:
   ```
   LOG_LEVEL=DEBUG
   ```

2. Use verbose flag with scripts:
   ```bash
   python parse_resume.py --verbose
   ```

### Finding Log Files

Log files are stored in the `logs/` directory:
- `app.log`: General application logs
- `parser.log`: Resume parsing logs
- `search.log`: Search operation logs
- `error.log`: Error logs

### Profiling Performance

To identify bottlenecks:

```bash
python -m cProfile -o profile.stats parse_resume.py --file resume.pdf
python scripts/analyze_profile.py --stats profile.stats
```

## Common Command-Line Fixes

### Force Reindex All Resumes

```bash
python parse_resume.py --reindex --force
```

### Reset OpenSearch Index

```bash
python scripts/initialize_opensearch.py --reset
```

### Test System Components

```bash
python scripts/verify_installation.py --component opensearch
python scripts/verify_installation.py --component bedrock
python scripts/verify_installation.py --component s3
```

## Getting Help

If you continue to experience issues:

1. Check the [GitHub repository](https://github.com/username/resume-parser) for known issues
2. Include the following information when reporting problems:
   - Exact error message and stack trace
   - System information (OS, Python version)
   - Steps to reproduce the issue
   - Log files (with sensitive information removed) 