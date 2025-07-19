import os
import boto3
import logging
import tempfile
import io
from pathlib import Path
from botocore.exceptions import ClientError
from typing import List, Dict, Any, Optional, Generator, Tuple

# Try to import from aws_credentials.py first, then fall back to config.py
try:
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from aws_credentials import (
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_REGION
    )
    # Import other config variables from config
    from config.config import (
        S3_BUCKET_NAME, 
        S3_RAW_PREFIX, 
        S3_PROCESSED_PREFIX, 
        S3_ERROR_PREFIX
    )
    logging.info("Using AWS credentials from aws_credentials.py")
except ImportError:
    # Fall back to config.py
    from config.config import (
        AWS_REGION, 
        S3_BUCKET_NAME, 
        S3_RAW_PREFIX, 
        S3_PROCESSED_PREFIX, 
        S3_ERROR_PREFIX
    )
    # These might not be in config.py
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None

logger = logging.getLogger(__name__)

class S3Handler:
    """Handler for S3 operations."""
    
    def __init__(self, bucket_name: Optional[str] = None, region: Optional[str] = None):
        """
        Initialize S3 handler
        
        Args:
            bucket_name: S3 bucket name (defaults to config.S3_BUCKET_NAME)
            region: AWS region (defaults to config.AWS_REGION)
        """
        self.bucket_name = bucket_name or S3_BUCKET_NAME
        self.region = region or AWS_REGION
        
        if not self.bucket_name:
            raise ValueError("S3 bucket name is required but not found in environment. "
                             "Please add SOURCE_BUCKET=your-bucket-name to your .env file.")
            
        # Initialize S3 client with explicit credentials if available
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            logger.info(f"Using explicit AWS credentials: {AWS_ACCESS_KEY_ID[:4]}...{AWS_ACCESS_KEY_ID[-4:]}")
            self.s3_client = boto3.client(
                's3', 
                region_name=self.region,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
        else:
            # Fall back to default credentials
            logger.info("Using default AWS credentials from environment or config")
            self.s3_client = boto3.client('s3', region_name=self.region)
        
        logger.info(f"Initialized S3 handler for bucket: {self.bucket_name}")
    
    def list_objects(self, prefix: str, file_extensions: List[str] = None) -> Generator[Dict[str, Any], None, None]:
        """
        List objects in S3 bucket with given prefix
        
        Args:
            prefix: S3 key prefix to list objects from
            file_extensions: List of file extensions to filter (without dot, e.g. 'pdf')
            
        Yields:
            S3 object metadata dictionaries
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # Skip if it's a directory (ends with /)
                        if key.endswith('/'):
                            continue
                            
                        # Filter by file extension if provided
                        if file_extensions:
                            ext = key.split('.')[-1].lower() if '.' in key else ''
                            if ext not in file_extensions:
                                continue
                        
                        yield obj
        
        except ClientError as e:
            logger.error(f"Error listing objects from {prefix}: {str(e)}")
            raise
    
    def download_file(self, key: str, local_path: Optional[str] = None) -> str:
        """
        Download file from S3 to local filesystem or temporary location
        
        Args:
            key: S3 object key
            local_path: Local file path to save to (optional)
            
        Returns:
            Path to the downloaded file
        """
        if not local_path:
            # Create a temporary file with the same extension
            _, ext = os.path.splitext(key)
            filename = key.split('/')[-1]
            
            # Create a temporary file in the system's temp directory
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as temp_file:
                local_path = temp_file.name
        
        # Ensure the directory exists for the specified path
        if local_path:
            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
        
        try:
            logger.info(f"Downloading {key} to {local_path}")
            self.s3_client.download_file(self.bucket_name, key, local_path)
            return local_path
        
        except ClientError as e:
            logger.error(f"Error downloading {key}: {str(e)}")
            raise
    
    def get_object_bytes(self, key: str) -> io.BytesIO:
        """
        Download file from S3 to memory buffer
        
        Args:
            key: S3 object key
            
        Returns:
            BytesIO buffer containing the file contents
        """
        try:
            logger.info(f"Downloading {key} to memory")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            file_content = response['Body'].read()
            return io.BytesIO(file_content)
        
        except ClientError as e:
            logger.error(f"Error downloading {key} to memory: {str(e)}")
            raise
    
    def upload_file(self, local_path: str, key: str, extra_args: Dict[str, Any] = None) -> bool:
        """
        Upload file from local filesystem to S3
        
        Args:
            local_path: Path to local file
            key: S3 object key
            extra_args: Extra arguments for S3 upload (optional)
            
        Returns:
            True if successful
        """
        try:
            logger.info(f"Uploading {local_path} to s3://{self.bucket_name}/{key}")
            self.s3_client.upload_file(local_path, self.bucket_name, key, ExtraArgs=extra_args)
            return True
        
        except ClientError as e:
            logger.error(f"Error uploading {local_path} to {key}: {str(e)}")
            return False
    
    def upload_content(self, content: str, key: str, content_type: str = 'application/json') -> bool:
        """
        Upload string content directly to S3
        
        Args:
            content: String content to upload
            key: S3 object key
            content_type: Content type (default: application/json)
            
        Returns:
            True if successful
        """
        try:
            logger.info(f"Uploading content to s3://{self.bucket_name}/{key}")
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=content,
                ContentType=content_type
            )
            return True
        
        except ClientError as e:
            logger.error(f"Error uploading content to {key}: {str(e)}")
            return False
    
    def file_exists(self, key: str) -> bool:
        """
        Check if a file exists in S3
        
        Args:
            key: S3 object key
            
        Returns:
            True if file exists
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        
        except ClientError as e:
            # If error code is 404, file doesn't exist
            if e.response['Error']['Code'] == '404':
                return False
            # For other errors, log and re-raise
            logger.error(f"Error checking if {key} exists: {str(e)}")
            raise
    
    def get_object_metadata(self, key: str) -> Dict[str, Any]:
        """
        Get metadata for S3 object
        
        Args:
            key: S3 object key
            
        Returns:
            Object metadata dictionary
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return response
        
        except ClientError as e:
            logger.error(f"Error getting metadata for {key}: {str(e)}")
            raise
    
    def list_resume_files(self, prefix: str = S3_RAW_PREFIX, extensions: List[str] = None) -> Generator[Dict[str, Any], None, None]:
        """
        List resume files in the S3 bucket
        
        Args:
            prefix: S3 key prefix (default: S3_RAW_PREFIX)
            extensions: List of file extensions to include (default: pdf, docx, doc, txt)
            
        Yields:
            S3 object metadata dictionaries
        """
        if extensions is None:
            extensions = ['pdf', 'docx', 'doc', 'txt', 'rtf']
        
        return self.list_objects(prefix, extensions)
    
    def get_processed_key(self, raw_key: str) -> str:
        """
        Convert raw S3 key to processed S3 key
        
        Args:
            raw_key: Raw S3 key
            
        Returns:
            Processed S3 key
        """
        # Remove raw prefix and add processed prefix
        relative_path = raw_key.replace(S3_RAW_PREFIX, '', 1)
        return f"{S3_PROCESSED_PREFIX}{relative_path}"
    
    def get_error_key(self, raw_key: str) -> str:
        """
        Convert raw S3 key to error S3 key
        
        Args:
            raw_key: Raw S3 key
            
        Returns:
            Error S3 key
        """
        # Remove raw prefix and add error prefix
        relative_path = raw_key.replace(S3_RAW_PREFIX, '', 1)
        filename = os.path.splitext(os.path.basename(relative_path))[0]
        return f"{S3_ERROR_PREFIX}{os.path.dirname(relative_path)}/{filename}_error.json"
    
    def download_resume(self, s3_key: str) -> Tuple[str, str]:
        """
        Download resume file from S3
        
        Args:
            s3_key: S3 object key
            
        Returns:
            Tuple of (local file path, file type)
        """
        local_path = self.download_file(s3_key)
        file_type = os.path.splitext(s3_key)[1].lstrip('.').lower()
        return local_path, file_type 