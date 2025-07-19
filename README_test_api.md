# Resume Matching API Test Script

This script allows you to test the OpenSearch Resume Matching API with various job descriptions and displays the results in a readable format.

## Requirements

Install the required packages:

```bash
pip install requests python-dotenv rich
```

## Configuration

1. Make sure the `.env` file exists with the following variables:
   ```
   # Required: API Gateway URL
   OPENSEARCH_GATEWAY_API_URL=https://your-api-gateway-id.execute-api.region.amazonaws.com/stage/resume-matching
   
   # API key for authentication (required if API Gateway uses API key authorization)
   OPENSEARCH_REST_API_KEY=your-api-key-here
   ```

2. The script will save API responses to:
   ```
   C:\Users\MohanS\Downloads\resume_parser_NEW_FINAL_with_docs\resume_parser_NEW_FINAL_with_docs\output\API_output
   ```

## Authentication

If you receive a 403 error with "Missing Authentication Token" message, the API requires authentication. Add your API key in one of these ways:

1. In the `.env` file as `OPENSEARCH_REST_API_KEY` (recommended)
2. Using the `--api-key` command line parameter
3. Setting the `OPENSEARCH_REST_API_KEY` environment variable
4. The script also supports the legacy `API_KEY` variable name for backward compatibility

## Usage

### Basic Usage

```bash
python test_api.py
```
This will run the test with a default job description.

### Command-line Options

```bash
# Run with a custom job description
python test_api.py --job "Senior Python Developer with 5+ years experience in AWS Lambda and API Gateway"

# Run batch tests with predefined job descriptions
python test_api.py --batch

# Adjust the number of candidates to analyze
python test_api.py --analyze-count 10

# Analyze all candidates (may take longer)
python test_api.py --analyze-all

# Provide a custom API URL
python test_api.py --url "https://your-custom-api-url"

# Provide an API key for authentication
python test_api.py --api-key "your-api-key-here" 

# Use POST method instead of GET (important for many API Gateway endpoints)
python test_api.py --method post

# Don't save API responses to files
python test_api.py --no-save
```

### Examples

Test a specific job description with API key authentication using POST method:
```bash
python test_api.py --job "Data Scientist with Python and ML experience" --api-key "your-api-key-here" --method post
```

Run all predefined test cases with POST method:
```bash
python test_api.py --batch --method post
```

## Output

The script outputs:
1. A summary of the API response including job title, required skills, and timing information
2. Detailed information about the top candidates including skills, experience, and professional analysis
3. JSON files with complete API responses in the output directory

Each test response is saved as a timestamped JSON file with a slug derived from the job description.

## Troubleshooting

- If you get a 403 error with "Missing Authentication Token" message:
  - Check that your API key is correct
  - Try using POST instead of GET with `--method post`
  - Verify the API Gateway URL format (it may need a trailing slash)
  - Check if additional path components are needed in the URL
  
- If you get a 404 "Not Found" error:
  - Verify the API endpoint URL is correct
  - Check if the resource path in the URL needs adjustment
  
- If you get a 500 "Internal Server Error":
  - The API service might be experiencing issues
  - Check the Lambda function logs in AWS CloudWatch

- If the output directory cannot be created, make sure you have write permissions for the directory 