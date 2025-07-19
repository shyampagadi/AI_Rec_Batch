
# Step-by-Step Guide: Creating API Gateway for Lambda Integration

## Part 1: Deploy the Lambda Function

1. **Log in to AWS Console**
   - Go to https://console.aws.amazon.com and sign in

2. **Create Lambda Function**
   - Navigate to Lambda service
   - Click "Create function"
   - Select "Author from scratch"
   - Enter name: `ResumeMatchingFunction`
   - Runtime: Python 3.12
   - Architecture: x86_64
   - Click "Create function"

3. **Upload Code**
   - In the Code tab, delete the example code
   - Upload a zip file containing:
     - `lambda_handler.py` 
     - A `requirements.txt` file listing: `opensearchpy`, `requests_aws4auth`
     - A Python virtual environment folder with these dependencies
           mkdir package
            pip install -t ./package opensearch-py>=2.3.0 requests-aws4auth>=1.1.1 boto3 python-dotenv

            # Copy your lambda function to the package directory
            cp lambda_function.py ./package/
   
            # Create a zip file (from within the package directory)
            cd package
            zip -r ../lambda_deployment.zip .
            powersell: Compress-Archive -Path * -DestinationPath ../lambda_deployment.zip

            cd ..
4. **Configure Environment Variables**
   - Scroll down to "Environment variables"
   - Add the following key-value pairs:
     - `OPENSEARCH_ENDPOINT` = [your-opensearch-endpoint]
     - `OPENSEARCH_INDEX` = resume-embeddings
     - `OPENSEARCH_REGION` = us-east-1
     - `OPENSEARCH_SERVERLESS` = true (if using OpenSearch Serverless)
     - `BEDROCK_EMBEDDINGS_MODEL` = amazon.titan-embed-text-v2:0
     - `BEDROCK_MODEL_ID` = meta.llama3-70b-instruct-v1:0

5. **Configure Permissions**
   - In the Configuration tab, click "Permissions"
   - Click on the execution role
   - Add these policies:
     - `AmazonOpenSearchServiceFullAccess` (or a custom policy with read access)
     - `AmazonBedrockFullAccess` (or a custom policy with invoke permissions)

6. **Configure Basic Settings**
   - Increase Timeout to 30 seconds
   - Increase Memory to 512 MB
   - Click "Save"

## Part 2: Create the API Gateway

1. **Navigate to API Gateway Service**
   - In AWS Console, go to API Gateway

2. **Create a REST API**
   - Click "Create API"
   - Select "REST API" (not HTTP API)
   - Choose "New API"
   - Enter API name: `ResumeMatchingAPI`
   - Description: "API to retrieve matching resumes based on job descriptions"
   - Endpoint Type: Regional
   - Click "Create API"

3. **Create Resources and Methods**
   - Click "Actions" dropdown → "Create Resource"
   - Resource Name: `matches`
   - Click "Create Resource"
   - Click "Actions" dropdown → "Create Method"
   - Select "POST" from the dropdown
   - Click the checkmark

4. **Configure POST Method**
   - Integration type: Lambda Function
   - Lambda Proxy integration: Check this box
   - Lambda Region: Select your region
   - Lambda Function: `ResumeMatchingFunction`
   - Click "Save"
   - When prompted to add permissions, click "OK"

5. **Configure Request Body**
   - Click on "Method Request"
   - Under "Request Body", click "Add model"
   - Content type: `application/json`
   - Model name: `ResumeMatchRequest`
   - Click checkmark
   - Go to "Models" in the left sidebar
   - Click "Create Model"
   - Model name: `ResumeMatchRequest`
   - Content type: application/json
   - Model schema:
     ```json
     {
       "type": "object",
       "required": ["job_description"],
       "properties": {
         "job_description": {"type": "string"},
         "max_results": {"type": "integer", "default": 10}
       }
     }
     ```
   - Click "Create model"

6. **Enable CORS**
   - Select the `matches` resource
   - Click "Actions" dropdown → "Enable CORS"
   - Check all options
   - Click "Enable CORS and replace existing CORS headers"

7. **Deploy the API**
   - Click "Actions" dropdown → "Deploy API"
   - Deployment stage: [New Stage]
   - Stage name: `prod`
   - Description: "Production deployment"
   - Click "Deploy"

8. **Get Invoke URL**
   - In the Stages section, expand "prod"
   - Select "POST" under `/matches`
   - Copy the Invoke URL (will look like: `https://xyz123.execute-api.region.amazonaws.com/prod/matches`)

## Part 3: Testing the Integration

1. **Testing in API Gateway Console**
   - In the API Gateway console, select your API
   - Click on "Resources"
   - Select the POST method under `/matches`
   - Click "Test"
   - In the Request Body field, enter:
     ```json
     {
       "job_description": "Senior Data Engineer with 5+ years experience in AWS, Python, and Snowflake",
       "max_results": 5
     }
     ```
   - Click "Test"
   - Verify you get a 200 response with resume IDs

2. **Testing with curl**
   ```bash
   curl -X POST \
     -H "Content-Type: application/json" \
     -d '{"job_description":"Senior Data Engineer with 5+ years experience in AWS, Python, and Snowflake","max_results":5}' \
     https://xyz123.execute-api.region.amazonaws.com/prod/matches
   ```

3. **Testing with Postman**
   - Create a new POST request
   - URL: Your Invoke URL
   - Headers: Content-Type: application/json
   - Body: Raw, JSON
     ```json
     {
       "job_description": "Senior Data Engineer with 5+ years experience in AWS, Python, and Snowflake",
       "max_results": 5
     }
     ```
   - Send and verify the response

## Part 4: Production Considerations

1. **API Key**
   - To secure your API, click "API Keys" in the left sidebar
   - Click "Actions" dropdown → "Create API key"
   - Name: `ResumeMatchingAPIKey`
   - Click "Save"
   - Go back to Resources → POST method → Method Request
   - Set "API Key Required" to true
   - Redeploy your API

2. **Usage Plans**
   - Click "Usage Plans" in the left sidebar
   - Create a usage plan with throttling limits
   - Associate your API key with this plan

3. **Monitoring**
   - Set up CloudWatch metrics for monitoring API calls
   - Configure logging for troubleshooting

4. **Custom Domain**
   - For a professional endpoint, configure a custom domain name
   - In API Gateway, go to "Custom Domain Names"
   - Follow the wizard to set up your domain with ACM certificate

By following these steps, you'll have a fully functional API Gateway integrated with your Lambda function for resume matching.
