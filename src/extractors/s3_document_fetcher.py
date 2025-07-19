import os
import logging
import boto3
import io
import tempfile
from typing import List, Dict, Generator, Any, Optional, Tuple
from botocore.exceptions import ClientError
from config.config import S3_BUCKET_NAME, S3_INPUT_PREFIX, S3_PROCESSED_PREFIX, get_aws_credentials

logger = logging.getLogger(__name__)

class S3DocumentFetcher:
    """Fetch documents from S3 bucket for processing"""
    
    def __init__(self, bucket_name: Optional[str] = None, input_prefix: Optional[str] = None):
        """
        Initialize S3 client and set bucket and prefix parameters
        
        Args:
            bucket_name: Name of S3 bucket (defaults to config value)
            input_prefix: Prefix for input documents (defaults to config value)
        """
        credentials = get_aws_credentials()
        self.s3_client = boto3.client('s3', **credentials)
        self.bucket_name = bucket_name or S3_BUCKET_NAME
        self.input_prefix = input_prefix or S3_INPUT_PREFIX
        
        logger.info(f"Initialized S3DocumentFetcher for bucket: {self.bucket_name}, prefix: {self.input_prefix}")
    
    def list_documents(self, max_keys: int = 1000, 
                      file_extensions: List[str] = ['.pdf', '.docx', '.doc', '.rtf']) -> List[Dict[str, Any]]:
        """
        List documents in S3 bucket with specified extensions
        
        Args:
            max_keys: Maximum number of objects to return
            file_extensions: List of file extensions to filter
            
        Returns:
            List of document metadata dictionaries
        """
        try:
            documents = []
            continuation_token = None
            
            while True:
                # Prepare list_objects_v2 parameters
                list_params = {
                    'Bucket': self.bucket_name,
                    'Prefix': self.input_prefix,
                    'MaxKeys': max_keys
                }
                
                # Add continuation token if we're paginating
                if continuation_token:
                    list_params['ContinuationToken'] = continuation_token
                
                # Request objects from S3
                response = self.s3_client.list_objects_v2(**list_params)
                
                # Process results
                if 'Contents' in response:
                    for obj in response['Contents']:
                        key = obj['Key']
                        # Check if file has one of the specified extensions
                        if any(key.lower().endswith(ext) for ext in file_extensions):
                            doc_info = {
                                's3_key': key,
                                'file_name': os.path.basename(key),
                                'file_size': obj['Size'],
                                'last_modified': obj['LastModified'],
                                'file_type': key.split('.')[-1].lower()
                            }
                            documents.append(doc_info)
                
                # Check if there are more results to fetch
                if not response.get('IsTruncated', False):
                    break
                
                continuation_token = response.get('NextContinuationToken')
            
            logger.info(f"Found {len(documents)} documents in S3 bucket")
            return documents
            
        except ClientError as e:
            logger.error(f"Error listing documents from S3: {str(e)}")
            raise
    
    def get_document_batch(self, batch_size: int = 100) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Get documents in batches for efficient processing
        
        Args:
            batch_size: Number of documents per batch
            
        Yields:
            Batches of document metadata dictionaries
        """
        documents = self.list_documents()
        
        # Yield documents in batches
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            logger.info(f"Yielding batch of {len(batch)} documents")
            yield batch
    
    def download_document(self, s3_key: str, local_path: Optional[str] = None) -> Tuple[str, str]:
        """
        Download a document from S3
        
        Args:
            s3_key: S3 key of the document to download
            local_path: Local path to save file (if None, uses system temp directory)
            
        Returns:
            Tuple of (local_file_path, file_type)
        """
        try:
            # Generate local path if not provided
            if local_path is None:
                file_name = os.path.basename(s3_key)
                file_extension = os.path.splitext(s3_key)[1]
                # Create a temporary file in the system temp directory
                with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as temp_file:
                    local_path = temp_file.name
            
            # Download file
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            
            # Determine file type
            file_extension = os.path.splitext(s3_key)[1].lower()
            file_type = file_extension.lstrip('.')
            
            logger.info(f"Downloaded document {s3_key} to {local_path}")
            return local_path, file_type
            
        except ClientError as e:
            logger.error(f"Error downloading document {s3_key}: {str(e)}")
            raise
    
    def get_document_bytes(self, s3_key: str) -> Tuple[io.BytesIO, str]:
        """
        Get document content as in-memory bytes from S3
        
        Args:
            s3_key: S3 key of the document to download
            
        Returns:
            Tuple of (BytesIO object, file_type)
        """
        try:
            # Get object from S3
            logger.info(f"Getting document content from S3: {s3_key}")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            file_content = response['Body'].read()
            
            # Determine file type
            file_extension = os.path.splitext(s3_key)[1].lower()
            file_type = file_extension.lstrip('.')
            
            logger.info(f"Retrieved document {s3_key} ({len(file_content)} bytes)")
            return io.BytesIO(file_content), file_type
            
        except ClientError as e:
            logger.error(f"Error getting document content from S3 {s3_key}: {str(e)}")
            raise
    
    def mark_as_processed(self, s3_key: str, success: bool = True) -> str:
        """
        Move processed document to processed prefix
        
        Args:
            s3_key: S3 key of the processed document
            success: Whether processing was successful
            
        Returns:
            New S3 key location
        """
        try:
            # Determine new key with processed prefix
            file_name = os.path.basename(s3_key)
            status_prefix = 'success/' if success else 'failed/'
            new_key = f"{S3_PROCESSED_PREFIX}{status_prefix}{file_name}"
            
            # Copy object to new location
            self.s3_client.copy_object(
                CopySource={'Bucket': self.bucket_name, 'Key': s3_key},
                Bucket=self.bucket_name,
                Key=new_key
            )
            
            # Delete original object
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            logger.info(f"Marked document {s3_key} as processed: {new_key}")
            return new_key
            
        except ClientError as e:
            logger.error(f"Error marking document {s3_key} as processed: {str(e)}")
            raise 