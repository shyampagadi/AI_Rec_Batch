import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from config.config import (
    ENABLE_OPENSEARCH,
    OPENSEARCH_ENDPOINT,
    OPENSEARCH_SERVERLESS,
    OPENSEARCH_COLLECTION_NAME,
    OPENSEARCH_INDEX,
    OPENSEARCH_REGION,
    OPENSEARCH_USERNAME,
    OPENSEARCH_PASSWORD,
    AWS_REGION,
    get_aws_credentials
)
from src.utils.bedrock_embeddings import BedrockEmbeddings

logger = logging.getLogger(__name__)

# Path to schema file
SCHEMA_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils', 'opensearch_schema.txt')

class OpenSearchHandler:
    """Handler for Amazon OpenSearch operations"""
    
    def __init__(self):
        """Initialize OpenSearch handler"""
        if not ENABLE_OPENSEARCH:
            logger.warning("OpenSearch is disabled in configuration")
            return
            
        self.endpoint = OPENSEARCH_ENDPOINT
        self.region = OPENSEARCH_REGION or AWS_REGION
        self.is_serverless = OPENSEARCH_SERVERLESS
        self.index_name = OPENSEARCH_INDEX or 'resume-embeddings'
        self.collection_name = OPENSEARCH_COLLECTION_NAME
        
        # Initialize embedding client for resume text embedding
        self.embeddings_client = BedrockEmbeddings()
        
        if not self.endpoint:
            logger.error("OpenSearch endpoint is not provided")
            raise ValueError("Missing OpenSearch endpoint")
        
        # Initialize the client
        self._initialize_client()
        
        # Create index if it doesn't exist
        self._create_index_if_not_exists()
        
        logger.info(f"Initialized OpenSearch handler for endpoint: {self.endpoint}, index: {self.index_name}")
    
    def _initialize_client(self):
        """Initialize the OpenSearch client with proper authentication"""
        try:
            # Service name is different for OpenSearch Serverless vs Domain
            service_name = 'aoss' if self.is_serverless else 'es'
            
            if self.is_serverless:
                # For OpenSearch Serverless
                session = boto3.Session()
                credentials = session.get_credentials()
                
                if credentials is None:
                    raise ValueError("Missing AWS credentials for OpenSearch Serverless")
                    
                awsauth = AWS4Auth(
                    credentials.access_key,
                    credentials.secret_key,
                    self.region,
                    service_name,
                    session_token=credentials.token
                )
                
                # Handle token issue with some versions of requests-aws4auth
                if hasattr(awsauth, 'auth') and 'X-Amz-Security-Token' in awsauth.auth:
                    awsauth.auth['X-Amz-Security-Token'] = credentials.token
            elif OPENSEARCH_USERNAME and OPENSEARCH_PASSWORD:
                # For OpenSearch Domain with Basic Auth
                self.client = OpenSearch(
                    hosts=[{'host': self.endpoint, 'port': 443}],
                    http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),
                    use_ssl=True,
                    verify_certs=True,
                    connection_class=RequestsHttpConnection,
                    timeout=30
                )
                return
            else:
                # For OpenSearch Domain with IAM
                credentials = get_aws_credentials()
                awsauth = AWS4Auth(
                    credentials.get('aws_access_key_id', ''),
                    credentials.get('aws_secret_access_key', ''),
                    self.region,
                    service_name
                )
            
            # Create client with auth
            self.client = OpenSearch(
                hosts=[{'host': self.endpoint, 'port': 443}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=30
            )
            
            # Test connection
            try:
                indices = self.client.indices.get_alias("*")
                logger.info(f"Successfully connected to OpenSearch. Found {len(indices)} indices.")
            except Exception as e:
                if '404' in str(e):
                    logger.info("Connected to OpenSearch (no indices found yet)")
                else:
                    logger.warning(f"Connection test warning: {str(e)[:300]}")
            
        except Exception as e:
            logger.error(f"Error initializing OpenSearch client: {str(e)[:300]}")
            raise
    
    def _create_index_if_not_exists(self):
        """Create the index if it doesn't exist using the schema file"""
        try:
            if not self.client.indices.exists(index=self.index_name):
                logger.info(f"Creating index {self.index_name} with schema from {SCHEMA_FILE}")
                
                try:
                    with open(SCHEMA_FILE, 'r') as f:
                        schema_content = f.read()
                        
                    # Extract the JSON part (after the first line)
                    lines = schema_content.split('\n', 1)
                    json_content = lines[1] if len(lines) > 1 else schema_content
                    
                    schema = json.loads(json_content)
                    
                    response = self.client.indices.create(
                        index=self.index_name,
                        body=schema,
                        ignore=400
                    )
                    
                    if response.get('acknowledged', False):
                        logger.info(f"Successfully created index {self.index_name}")
                    else:
                        logger.warning(f"Index creation response: {response}")
                except Exception as e:
                    logger.error(f"Failed to create index: {str(e)}")
                    raise
            else:
                logger.info(f"Index {self.index_name} already exists")
                
        except Exception as e:
            logger.error(f"Error checking/creating index: {str(e)}")
            raise
    
    def _prepare_document(self, resume_data: Dict[str, Any], resume_id: str, resume_text: str) -> Dict[str, Any]:
        """
        Prepare a document that matches the OpenSearch schema
        
        Args:
            resume_data: Resume data extracted by LLM
            resume_id: Unique identifier for the resume
            resume_text: Raw resume text
            
        Returns:
            Document ready for OpenSearch
        """
        # Create base document with metadata - only include fields defined in schema
        document = {
            'resume_id': resume_id,
            'collection_name': self.collection_name,
            'created_dt': datetime.now().isoformat(),
            'updated_dt': datetime.now().isoformat()
        }
        
        # Generate embedding for the entire resume
        try:
            embedding = self.embeddings_client.get_embedding(resume_text, dimension=1024)
            document['resume_embedding'] = embedding
            logger.info("Added resume embedding to document")
        except Exception as e:
            logger.warning(f"Could not generate resume embedding: {str(e)[:200]}")
        
        # Don't store the raw resume text - only the embedding
        # document['resume_text'] = resume_text  # Removed to follow schema
        
        # Handle total_experience as float
        if 'total_experience' in resume_data:
            try:
                document['total_experience'] = float(resume_data['total_experience'])
            except (ValueError, TypeError):
                logger.warning(f"Could not convert total_experience to float: {resume_data['total_experience']}")
        
        # Handle simple text/keyword fields according to schema
        for field in ['summary', 'positions', 'skills', 'industries', 'certifications']:
            if field in resume_data:
                # Extract text from complex structures if needed
                if isinstance(resume_data[field], dict):
                    if 'text' in resume_data[field]:
                        document[field] = resume_data[field]['text']
                    elif 'values' in resume_data[field]:
                        document[field] = resume_data[field]['values']
                    else:
                        # Use first non-embedding field found
                        for k, v in resume_data[field].items():
                            if k not in ['embedding', 'fields']:
                                document[field] = v
                                break
                else:
                    document[field] = resume_data[field]
        
        # Handle nested objects according to schema
        for nested_field in ['companies', 'projects', 'education', 'achievements']:
            if nested_field in resume_data and isinstance(resume_data[nested_field], list):
                document[nested_field] = []
                for item in resume_data[nested_field]:
                    new_item = {}
                    
                    # Only include fields defined in schema
                    if nested_field == 'companies':
                        schema_fields = ['name', 'duration', 'description', 'industry']
                    elif nested_field == 'projects':
                        schema_fields = ['name', 'description', 'technologies']
                    elif nested_field == 'education':
                        schema_fields = ['degree', 'institution', 'year']
                    elif nested_field == 'achievements':
                        schema_fields = ['company', 'description', 'category']
                    else:
                        schema_fields = []
                    
                    # Extract text from nested fields
                    for key, val in item.items():
                        if key not in schema_fields:
                            continue
                            
                        if isinstance(val, dict) and 'text' in val:
                            new_item[key] = val['text']
                        elif isinstance(val, dict) and 'values' in val:
                            new_item[key] = val['values']
                        elif key not in ['embedding', 'fields']:
                            new_item[key] = val
                    
                    document[nested_field].append(new_item)
        
        return document
    
    def store_resume(self, resume_data: Dict[str, Any], resume_id: str, resume_text: str) -> bool:
        """
        Store resume data in OpenSearch
        
        Args:
            resume_data: Resume data extracted by LLM
            resume_id: Unique identifier for the resume
            resume_text: Raw resume text
            
        Returns:
            True if successful, False otherwise
        """
        if not ENABLE_OPENSEARCH:
            logger.info("OpenSearch storage is disabled")
            return False
            
        try:
            logger.info(f"Storing resume data for resume_id={resume_id}")
            
            # Prepare document based on schema
            document = self._prepare_document(resume_data, resume_id, resume_text)
            
            # Store in OpenSearch with retries
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"Storing document in OpenSearch (attempt {attempt+1}/{max_retries})")
                    
                    if self.is_serverless:
                        response = self.client.index(
                            index=self.index_name,
                            body=document
                        )
                    else:
                        response = self.client.index(
                            index=self.index_name,
                            body=document,
                            id=resume_id,
                            refresh=True
                        )
                    
                    if response.get('result') in ('created', 'updated'):
                        logger.info(f"Successfully stored resume in OpenSearch with ID: {resume_id}")
                        return True
                    else:
                        logger.warning(f"Unexpected response: {json.dumps(response)[:200]}")
                        
                except Exception as e:
                    error_msg = str(e)[:300]
                    logger.warning(f"Error on attempt {attempt+1}/{max_retries}: {error_msg}")
                    
                    # If there's a schema issue, try with simplified document
                    if 'mapper_parsing_exception' in error_msg:
                        logger.info("Schema error detected, simplifying document")
                        # Remove problematic fields
                        if 'resume_embedding' in document:
                            logger.info("Removing resume_embedding field due to schema error")
                            del document['resume_embedding']
                    
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    else:
                        logger.error(f"All {max_retries} attempts failed")
                        return False
            
            return False
                
        except Exception as e:
            logger.error(f"Error storing resume: {str(e)[:300]}")
            return False
    
    def search_resumes(self, query: str, size: int = 10) -> List[Dict[str, Any]]:
        """
        Search for resumes using text query
        
        Args:
            query: Search query
            size: Maximum number of results to return
            
        Returns:
            List of matching resume documents
        """
        if not ENABLE_OPENSEARCH:
            return []
            
        try:
            search_query = {
                "size": size,
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "summary^3",
                            "positions^2",
                            "skills^2", 
                            "projects.description",
                            "companies.description",
                            "education.degree"
                        ],
                        "type": "best_fields"
                    }
                }
            }
            
            response = self.client.search(
                body=search_query,
                index=self.index_name
            )
            
            hits = response.get('hits', {}).get('hits', [])
            results = []
            
            for hit in hits:
                doc = hit.get('_source', {})
                # Remove resume_embedding from results to save bandwidth
                if 'resume_embedding' in doc:
                    del doc['resume_embedding']
                doc['score'] = hit.get('_score')
                results.append(doc)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching resumes: {str(e)[:300]}")
            return []
    
    def vector_search(self, query_text: str, size: int = 10) -> List[Dict[str, Any]]:
        """
        Search for resumes using vector similarity
        
        Args:
            query_text: Text to search for
            size: Maximum number of results to return
            
        Returns:
            List of matching resume documents
        """
        if not ENABLE_OPENSEARCH:
            return []
            
        try:
            # Generate embedding for the query text
            query_embedding = self.embeddings_client.get_embedding(query_text, dimension=1024)
            logger.info(f"Generated embedding for vector search")
            
            # Build the kNN query
            search_query = {
                "size": size,
                "query": {
                    "knn": {
                        "resume_embedding": {
                            "vector": query_embedding,
                            "k": size
                        }
                    }
                }
            }
            
            # Execute search
            response = self.client.search(
                body=search_query,
                index=self.index_name
            )
            
            # Process results
            hits = response.get('hits', {}).get('hits', [])
            results = []
            
            for hit in hits:
                doc = hit.get('_source', {})
                # Remove resume_embedding from results to save bandwidth
                if 'resume_embedding' in doc:
                    del doc['resume_embedding']
                doc['score'] = hit.get('_score')
                results.append(doc)
            
            logger.info(f"Vector search returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Error in vector search: {str(e)[:300]}")
            return []
    
    def hybrid_search(self, query_text: str, size: int = 10) -> List[Dict[str, Any]]:
        """
        Hybrid search combining text and vector search
        
        Args:
            query_text: Text to search for
            size: Maximum number of results to return
            
        Returns:
            List of matching resume documents
        """
        if not ENABLE_OPENSEARCH:
            return []
            
        try:
            # Generate embedding for the query text
            query_embedding = self.embeddings_client.get_embedding(query_text, dimension=1024)
            
            # Build hybrid query with text and vector components
            search_query = {
                "size": size,
                "query": {
                    "bool": {
                        "should": [
                            {
                                "multi_match": {
                                    "query": query_text,
                                    "fields": [
                                        "summary^3",
                                        "positions^2",
                                        "skills^2",
                                        "projects.description",
                                        "companies.description"
                                    ],
                                    "type": "best_fields",
                                    "boost": 0.5
                                }
                            },
                            {
                                "knn": {
                                    "resume_embedding": {
                                        "vector": query_embedding,
                                        "k": size,
                                        "boost": 0.5
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            
            # Execute search
            response = self.client.search(
                body=search_query,
                index=self.index_name
            )
            
            # Process results
            hits = response.get('hits', {}).get('hits', [])
            results = []
            
            for hit in hits:
                doc = hit.get('_source', {})
                # Remove resume_embedding from results to save bandwidth
                if 'resume_embedding' in doc:
                    del doc['resume_embedding']
                doc['score'] = hit.get('_score')
                results.append(doc)
            
            logger.info(f"Hybrid search returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Error in hybrid search: {str(e)[:300]}")
            return []
    
    def get_resume(self, resume_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a resume by ID"""
        if not ENABLE_OPENSEARCH:
            return None
            
        try:
            response = self.client.get(
                index=self.index_name,
                id=resume_id
            )
            
            if response.get('found'):
                return response.get('_source')
            else:
                logger.warning(f"Resume with ID {resume_id} not found")
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving resume: {str(e)[:300]}")
            return None
    
    def delete_resume(self, resume_id: str) -> bool:
        """Delete a resume by ID"""
        if not ENABLE_OPENSEARCH:
            return False
            
        try:
            response = self.client.delete(
                index=self.index_name,
                id=resume_id,
                refresh=not self.is_serverless
            )
            
            if response.get('result') == 'deleted':
                logger.info(f"Successfully deleted resume with ID: {resume_id}")
                return True
            else:
                logger.warning(f"Unexpected response when deleting resume: {response}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting resume: {str(e)[:300]}")
            return False 