#!/bin/bash

# This script builds and pushes the resume parser Docker image to Amazon ECR
# Prerequisites: AWS CLI must be configured and you must have appropriate permissions

# Configuration - change these values
AWS_REGION="${AWS_REGION:-us-east-1}"
ECR_REPOSITORY_NAME="${ECR_REPOSITORY_NAME:-resume-parser}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Full ECR repository URI
ECR_REPOSITORY_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY_NAME}"

echo "Building and pushing to: ${ECR_REPOSITORY_URI}:${IMAGE_TAG}"

# Ensure the ECR repository exists
echo "Checking if ECR repository exists..."
if ! aws ecr describe-repositories --repository-names "${ECR_REPOSITORY_NAME}" --region "${AWS_REGION}" &> /dev/null; then
    echo "Creating ECR repository: ${ECR_REPOSITORY_NAME}"
    aws ecr create-repository --repository-name "${ECR_REPOSITORY_NAME}" --region "${AWS_REGION}"
fi

# Login to ECR
echo "Logging in to Amazon ECR..."
aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Build the Docker image
echo "Building Docker image..."
docker build -t "${ECR_REPOSITORY_NAME}:${IMAGE_TAG}" -f Dockerfile ..

# Tag the image for ECR
echo "Tagging image for ECR..."
docker tag "${ECR_REPOSITORY_NAME}:${IMAGE_TAG}" "${ECR_REPOSITORY_URI}:${IMAGE_TAG}"

# Push the image to ECR
echo "Pushing image to ECR..."
docker push "${ECR_REPOSITORY_URI}:${IMAGE_TAG}"

echo "Image successfully pushed to ECR: ${ECR_REPOSITORY_URI}:${IMAGE_TAG}"
echo
echo "To run this image on AWS Batch or ECS, use the following image URI:"
echo "${ECR_REPOSITORY_URI}:${IMAGE_TAG}"
echo
echo "Example AWS Batch job definition snippet:"
echo "{
    \"jobDefinitionName\": \"resume-parser-job\",
    \"type\": \"container\",
    \"containerProperties\": {
        \"image\": \"${ECR_REPOSITORY_URI}:${IMAGE_TAG}\",
        \"vcpus\": 2,
        \"memory\": 4096,
        \"command\": [
            \"python\", 
            \"parse_resume.py\",
            \"--batch\",
            \"--max_files\", 
            \"100\"
        ],
        \"environment\": [
            {\"name\": \"AWS_REGION\", \"value\": \"${AWS_REGION}\"},
            {\"name\": \"S3_BUCKET_NAME\", \"value\": \"YOUR_BUCKET_NAME\"},
            {\"name\": \"ENABLE_OPENSEARCH\", \"value\": \"true\"}
        ]
    }
}" 