#!/usr/bin/env python
"""
Resume Retrieval System

This script provides functionality to search and retrieve resumes matching job descriptions
using vector similarity, text search, or hybrid approaches.

Model Usage:
- BEDROCK_EMBEDDINGS_MODEL: Used exclusively for generating vector embeddings
  (typically amazon.titan-embed-text-v2:0)
- BEDROCK_MODEL_ID (MODEL_ID): Used exclusively for LLM-based parsing and analysis
  (typically anthropic.claude-*, meta.llama-*, etc.)

Key Functions:
- search_resumes_by_jd: Main entry point for searching resumes
- _vector_search: Performs vector search with optional reranking
- _get_embedding: Creates vector embeddings for text using BEDROCK_EMBEDDINGS_MODEL
- extract_jd_info_llm: Extracts structured information from job descriptions using MODEL_ID
- _extract_skills_llm: Extracts skills from text using MODEL_ID
"""
import os
import json
import logging
import argparse
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional
import uuid
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import math

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import handlers - these are still needed for other functionality
from src.storage.opensearch_handler import OpenSearchHandler
from src.storage.postgres_handler import PostgresHandler
from src.storage.dynamodb_handler import DynamoDBHandler
from config.config import LOCAL_OUTPUT_DIR

class ResumeRetriever:
    """Class to retrieve and rank resumes based on job description"""
    
    def __init__(self):
        """Initialize handlers for data retrieval"""
        self.opensearch = OpenSearchHandler()
        self.postgres = PostgresHandler()
        self.dynamodb = DynamoDBHandler()
        
        # Set output directory to the specified path or use a default
        self.output_dir = LOCAL_OUTPUT_DIR or "output"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create a subdirectory for LLM outputs
        self.llm_output_dir = os.path.join(self.output_dir, "llm_outputs")
        os.makedirs(self.llm_output_dir, exist_ok=True)
        
        # Add caches for search results - improves efficiency for repeated searches
        self._text_search_cache = {}
        self._vector_search_cache = {}
        self._hybrid_search_cache = {}
        self._embedding_cache = {}
        
        logger.info("Resume retriever initialized")
    
    def _get_cache_key(self, query: str, size: int, method: str) -> str:
        """Generate a unique cache key for search results"""
        import hashlib
        # Create a hash of the query, size and method to use as a cache key
        return hashlib.md5(f"{query}:{size}:{method}".encode()).hexdigest()

    def search_resumes_by_jd(self, job_description: str, max_results: int = 20, jd_info: Optional[Dict[str, Any]] = None, enable_reranking: bool = True, search_method: str = None) -> List[Dict[str, Any]]:
        """
        Search for resumes matching the job description using hybrid search
        
        Args:
            job_description: The job description text
            max_results: Maximum number of results to return
            jd_info: Pre-extracted job description information (to avoid duplicate LLM calls)
            enable_reranking: Whether to apply reranking (improves relevance)
            search_method: Ignored parameter (kept for backward compatibility)
            
        Returns:
            List of matching resume documents with scores
        """
        # Quick validation to avoid unnecessary processing
        if not job_description or not job_description.strip():
            logger.warning("Empty job description provided")
            return []
            
        # Create cache key for this search request
        cache_key = self._get_cache_key(job_description, max_results, "hybrid")
        
        # Check if results are already cached
        if cache_key in self._hybrid_search_cache:
            logger.info("Returning cached hybrid search results")
            return self._hybrid_search_cache[cache_key]
            
        logger.info(f"Searching for resumes using hybrid search...")
        
        # Check if OpenSearch is properly initialized
        if not hasattr(self.opensearch, '_client') or self.opensearch._client is None:
            try:
                # Force initialization of the client
                logger.info("Initializing OpenSearch client...")
                self.opensearch._initialize_client()
            except Exception as e:
                logger.error(f"Failed to initialize OpenSearch client: {str(e)}")
                return []
        
        try:
            # Extract the most relevant information from the job description
            # only if not already provided (avoid redundant LLM calls)
            if jd_info is None:
                jd_info = self.extract_jd_info_llm(job_description)
            
            # Create a focused query from the job description - this helps narrow search
            focused_query = self._create_focused_search_query(job_description, jd_info)
            
            # Execute hybrid search with fallback to text search
            try:
                results = self._hybrid_search(focused_query, size=max_results)
                if not results:
                    logger.warning("Hybrid search returned no results, falling back to text search")
                    results = self._text_search(job_description, size=max_results)
            except Exception as hybrid_error:
                logger.warning(f"Hybrid search failed: {str(hybrid_error)}. Falling back to text search.")
                results = self._text_search(job_description, size=max_results)
                
            # Cache results
            self._hybrid_search_cache[cache_key] = results
            
            logger.info(f"Found {len(results)} matching resumes")
            return results
            
        except Exception as e:
            logger.error(f"Error searching resumes: {str(e)}")
            return []
    
    def _text_search(self, query: str, size: int = 10) -> List[Dict[str, Any]]:
        """
        Search for resumes using text query
        
        Args:
            query: Search query
            size: Maximum number of results to return
            
        Returns:
            List of matching resume documents
        """
        # Return early for empty queries
        if not query or not query.strip():
            return []
            
        try:
            # Create optimized search query with field boosting
            # Fields with higher boost (^N) have more influence on relevance score
            search_query = {
                "size": size,
                "query": {
                    "bool": {
                        "should": [
                            # Prioritize exact skill matches
                            {
                                "match": {
                                    "skills": {
                                        "query": query,
                                        "boost": 3.0
                                    }
                                }
                            },
                            # Give high weight to position/title matches
                            {
                                "match": {
                                    "positions": {
                                        "query": query,
                                        "boost": 2.5
                                    }
                                }
                            },
                            # Prioritize summary matches
                            {
                                "match": {
                                    "summary": {
                                        "query": query,
                                        "boost": 2.0
                                    }
                                }
                            },
                            # Search across multiple fields with standard boost
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "skills^2",
                                        "positions^2",
                                        "summary^1.5",
                                        "projects.description",
                                        "companies.description",
                                        "education.degree"
                                    ],
                                    "type": "best_fields",
                                    "tie_breaker": 0.3,
                                    "fuzziness": "AUTO:4,7"
                                }
                            }
                        ],
                        "minimum_should_match": 1
                    }
                },
                "_source": {
                    "excludes": ["resume_embedding"]  # Don't return the large embedding vector
                }
            }
            
            # Execute search with timeout for better responsiveness
            response = self.opensearch.client.search(
                body=search_query,
                index=self.opensearch.index_name,
                request_timeout=10  # 10-second timeout
            )
            
            # Efficiently process and normalize results
            hits = response.get('hits', {}).get('hits', [])
            results = []
            
            # Find max score for normalization if hits exist
            max_score = max([hit.get('_score', 0) for hit in hits]) if hits else 1
            
            for hit in hits:
                doc = hit.get('_source', {})
                
                # Remove embedding to reduce payload size
                if 'resume_embedding' in doc:
                    del doc['resume_embedding']
                    
                # Normalize score to 0-100 scale for consistent UI representation
                raw_score = hit.get('_score', 0)
                doc['score'] = min(round((raw_score / max_score) * 100, 2), 100)
                doc['raw_score'] = raw_score
                
                results.append(doc)
            
            return results
            
        except Exception as e:
            logger.error(f"Error in text search: {str(e)[:300]}")
            return []
    
    def _get_embedding(self, text: str, dimension: int = 1024) -> List[float]:
        """
        Get embedding for text with caching
        
        Uses BEDROCK_EMBEDDINGS_MODEL for generating embeddings, NOT MODEL_ID.
        
        Args:
            text: Text to embed
            dimension: Vector dimension
            
        Returns:
            Embedding vector
        """
        # Create cache key
        import hashlib
        cache_key = hashlib.md5(text.encode()).hexdigest()
        
        # Return cached embedding if available
        if cache_key in self._embedding_cache:
            logger.info("Using cached embedding")
            return self._embedding_cache[cache_key]
            
        # Generate new embedding
        try:
            from config.config import BEDROCK_EMBEDDINGS_MODEL, AWS_REGION
            
            # Initialize the Bedrock client
            bedrock_runtime = boto3.client(
                service_name="bedrock-runtime",
                region_name=AWS_REGION
            )
            
            # Use the correct model ID for embedding generation - not the LLM model
            embedding_model_id = BEDROCK_EMBEDDINGS_MODEL or "amazon.titan-embed-text-v2:0"
            
            # Ensure model ID has the version suffix
            if embedding_model_id and not embedding_model_id.endswith(":0"):
                embedding_model_id += ":0"
            
            logger.info(f"Using embedding model: {embedding_model_id}")
            
            # Use the correct request format based on the model
            # For amazon.titan-embed-text models
            if "titan-embed-text-v2" in embedding_model_id.lower():
                request_body = {
                    "inputText": text,
                    "dimensions": dimension  # Pass dimension parameter correctly
                }
            elif "titan-embed" in embedding_model_id.lower():
                request_body = {
                    "inputText": text
                }
            # For anthropic.claude-3 models
            elif "claude" in embedding_model_id.lower():
                request_body = {
                    "input_text": text,
                    "embedding_model": "default" 
                }
            # For cohere models
            elif "cohere" in embedding_model_id.lower():
                request_body = {
                    "texts": [text],
                    "input_type": "search_document"
                }
            else:
                # Default to Titan format
                request_body = {
                    "inputText": text
                }
            
            # Call the model
            logger.info(f"Generating embedding with model {embedding_model_id}")
            response = bedrock_runtime.invoke_model(
                modelId=embedding_model_id,
                body=json.dumps(request_body)
            )
            
            response_body = json.loads(response.get('body').read())
            
            # Handle response format based on the model
            if "titan-embed" in embedding_model_id.lower():
                embedding = response_body.get('embedding')
            elif "claude" in embedding_model_id.lower():
                embedding = response_body.get('embedding')
            elif "cohere" in embedding_model_id.lower():
                embedding = response_body.get('embeddings')[0]
            else:
                embedding = response_body.get('embedding', [])
            
            # Cache the embedding
            self._embedding_cache[cache_key] = embedding
            
            logger.info(f"Successfully generated embedding with {len(embedding) if embedding else 0} dimensions")
            return embedding
            
        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)[:300]}")
            # Return zero vector as fallback (will give poor results but won't crash)
            return [0.0] * dimension
    
    def _vector_search(self, query_text: str, size: int = 10, enable_reranking: bool = True) -> List[Dict[str, Any]]:
        """
        Search for resumes using vector similarity with optional reranking
        
        Uses BEDROCK_EMBEDDINGS_MODEL for generating embeddings via _get_embedding.
        
        Args:
            query_text: Text to search for
            size: Maximum number of results to return
            enable_reranking: Whether to apply reranking (improves relevance but adds computation)
            
        Returns:
            List of matching resume documents
        """
        # Return early for empty queries
        if not query_text or not query_text.strip():
            return []
            
        try:
            # Import the standardized text function to format the query text
            from src.utils.bedrock_embeddings import create_standardized_text
            
            # Parse JD info to get more structured data
            jd_info = self.extract_jd_info_llm(query_text)
            
            # Create query structure for standardized text generation
            query_structure = {
                "summary": query_text,
                "job_title": jd_info.get("job_title", ""),
                "required_skills": jd_info.get("required_skills", []),
                "required_experience": jd_info.get("required_experience", 0),
                "nice_to_have_skills": jd_info.get("nice_to_have_skills", [])
            }
            
            # Generate standardized text for more consistent embeddings
            structured_query = create_standardized_text(query_structure)
            
            # Get embedding with caching
            query_embedding = self._get_embedding(structured_query, dimension=1024)
            
            # Log embedding dimension
            logger.info(f"Using query embedding with dimension {len(query_embedding)}")
            
            # Check if embedding is valid
            if all(x == 0 for x in query_embedding):
                logger.error("Error: Generated embedding is all zeros, falling back to text search")
                return self._text_search(query_text, size=size)
            
            # Get more results than needed for reranking or pure vector search filtering
            # Get 3x the requested results, with a minimum of 30 and maximum of 100
            initial_size = min(max(size * 3, 30), 100) 
            logger.info(f"Retrieving {initial_size} initial results for {'reranking' if enable_reranking else 'filtering'}")
            
            search_query = {
                "size": initial_size,
                "query": {
                    "knn": {
                        "resume_embedding": {
                            "vector": query_embedding,
                            "k": initial_size
                        }
                    }
                },
                "_source": {
                    "excludes": ["resume_embedding"]  # Don't return the large embedding vector
                }
            }
            
            # Execute search with timeout
            response = self.opensearch.client.search(
                body=search_query,
                index=self.opensearch.index_name,
                request_timeout=15  # 15-second timeout for vector search
            )
            
            # Process results with optimized score normalization
            hits = response.get('hits', {}).get('hits', [])
            
            if not hits:
                return []
                
            # Initial results based on vector similarity  
            initial_results = []
            
            # Find min and max scores for better normalization
            scores = [hit.get('_score', 0) for hit in hits]
            max_score = max(scores)
            min_score = min(scores)
            score_range = max(max_score - min_score, 0.0001)  # Avoid division by zero
            
            for hit in hits:
                doc = hit.get('_source', {})
                
                # Remove embedding to reduce payload size
                if 'resume_embedding' in doc:
                    del doc['resume_embedding']
                    
                # Normalize score to 0-100 scale with min-max scaling
                raw_score = hit.get('_score', 0)
                normalized_score = ((raw_score - min_score) / score_range) * 100
                
                # Apply sigmoid normalization for more useful distribution
                relevance_factor = 15.0  # Higher values make scores more aggressive
                normalized_score = 100 * (1 / (1 + math.exp(-((normalized_score/100 - 0.5) * relevance_factor))))
                
                # Round to 2 decimal places and ensure 0-100 range
                normalized_score = min(round(normalized_score, 2), 100)
                
                # For pure vector search, use the normalized score as the final score
                if not enable_reranking:
                    doc['score'] = normalized_score
                else:
                    # For reranking, store as vector_score to be used in reranking
                    doc['vector_score'] = normalized_score
                
                doc['raw_score'] = raw_score
                initial_results.append(doc)
            
            # If reranking is disabled, just sort by vector score and return
            if not enable_reranking:
                logger.info("Using pure vector similarity ranking (reranking disabled)")
                initial_results.sort(key=lambda x: x['score'], reverse=True)
                initial_results = initial_results[:size]  # Limit to requested size
                return initial_results
            
            # RERANKING: Apply secondary ranking criteria to improve recall when enabled
            logger.info(f"Reranking {len(initial_results)} initial results")
            
            # Extract skills from query to use in reranking
            jd_skills = jd_info.get("required_skills", [])
            if not jd_skills:
                jd_skills = self.extract_skills_from_jd(query_text)
            
            # Rerank using secondary criteria
            reranked_results = []
            for resume in initial_results:
                # Get resume skills
                resume_skills = []
                if 'skills' in resume:
                    if isinstance(resume['skills'], list):
                        resume_skills = resume['skills']
                    elif isinstance(resume['skills'], str):
                        resume_skills = [resume['skills']]
                
                # Calculate skill match score if we have skills to match
                skill_score = 0
                if jd_skills and resume_skills:
                    skill_score = self.calculate_skill_match_score(resume_skills, jd_skills)
                
                # Calculate experience match if we have experience data to match
                exp_score = 0
                if 'total_experience' in resume and jd_info.get("required_experience", 0) > 0:
                    resume_exp = float(resume.get('total_experience', 0))
                    jd_exp = float(jd_info.get("required_experience", 0))
                    exp_score = self.calculate_experience_match(resume_exp, jd_exp)
                
                # Calculate position match if we have position data
                position_score = 0
                if jd_info.get("job_title") and 'positions' in resume:
                    job_title = jd_info.get("job_title", "").lower()
                    resume_positions = resume['positions'] if isinstance(resume['positions'], list) else [resume['positions']]
                    
                    for position in resume_positions:
                        position_lower = position.lower() if position else ""
                        if position_lower and job_title == position_lower:
                            position_score = 100
                            break
                        elif position_lower and (job_title in position_lower or position_lower in job_title):
                            position_score = max(position_score, 70)
                
                # Calculate combined rerank score - weights should sum to 1.0
                vector_weight = 0.40    # Weight for vector similarity
                skill_weight = 0.30     # Weight for skill match
                position_weight = 0.10  # Weight for position/title match
                exp_weight = 0.20       # Weight for experience match
                
                rerank_score = (
                    resume['vector_score'] * vector_weight +
                    skill_score * skill_weight +
                    position_score * position_weight +
                    exp_score * exp_weight
                )
                
                # Store scores in the result
                resume['rerank_score'] = min(round(rerank_score, 2), 100)
                resume['skill_score'] = skill_score
                resume['exp_score'] = exp_score
                resume['position_score'] = position_score
                
                reranked_results.append(resume)
            
            # Sort by reranked score and limit to requested size
            reranked_results.sort(key=lambda x: x['rerank_score'], reverse=True)
            reranked_results = reranked_results[:size]
            
            return reranked_results
            
        except Exception as e:
            logger.error(f"Error in vector search: {str(e)[:300]}")
            # Try text search as fallback
            logger.warning("Vector search failed, falling back to text search")
            return self._text_search(query_text, size=size)
    
    def _extract_query_structure(self, query_text: str, query_structure: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from query text for better embedding
        
        Args:
            query_text: The query text (likely a job description)
            query_structure: Base structure to populate
            
        Returns:
            Updated query structure with extracted information
        """
        try:
            import re
            
            # Extract skills with optimized pattern
            skills_patterns = [
                r'(?:required skills|skills required|skills|expertise|proficienc(?:y|ies))[:;]\s*(.*?)(?:\n\n|\n[A-Z]|$)',
                r'(?:technical skills|technical requirements)[:;]\s*(.*?)(?:\n\n|\n[A-Z]|$)',
                r'(?:qualifications)[:;]\s*(.*?)(?:\n\n|\n[A-Z]|$)'
            ]
            
            # Try each pattern until we find skills
            skills = []
            for pattern in skills_patterns:
                skills_match = re.search(pattern, query_text, re.IGNORECASE)
                if skills_match:
                    skills_text = skills_match.group(1)
                    # Split by common separators
                    skill_candidates = [s.strip() for s in re.split(r'[,;•\n]', skills_text) if s.strip()]
                    
                    # Filter out non-skills (too short/long)
                    skills = [s for s in skill_candidates if 2 < len(s) < 50]
                    if skills:
                        break
            
            if skills:
                query_structure["skills"] = skills
            
            # Extract experience with improved pattern
            exp_patterns = [
                r'(\d+[-+]?\s*(?:years?|yrs?))\s+(?:of)?\s+(?:experience|work)',
                r'(?:experience|years)[:;]?\s*(\d+[-+]?\s*(?:years?|yrs?)?)',
                r'(\d+[-+]?\s*(?:years?|yrs?))\s+experience'
            ]
            
            for pattern in exp_patterns:
                exp_match = re.search(pattern, query_text, re.IGNORECASE)
                if exp_match:
                    exp_text = exp_match.group(1)
                    # Extract just the number
                    exp_number = re.search(r'(\d+)', exp_text)
                    if exp_number:
                        query_structure["total_experience"] = int(exp_number.group(1))
                        break
            
            # Extract job title with improved pattern - look at start or after "Position:" etc.
            title_patterns = [
                r'^([^\n]{5,100})(?:\n|$)',
                r'(?:position|job title|role)[:;]\s*([^\n]{5,100})(?:\n|$)',
                r'(?:hiring for|looking for)[:;]?\s*([^\n]{5,100})(?:\n|$)'
            ]
            
            for pattern in title_patterns:
                title_match = re.search(pattern, query_text, re.IGNORECASE) 
                if title_match:
                    title = title_match.group(1).strip()
                    if title and 5 < len(title) < 100:  # Reasonable title length
                        query_structure["positions"] = [title]
                        break
                        
        except Exception as e:
            logger.warning(f"Error extracting query structure: {str(e)}")
            
        return query_structure
    
    def _hybrid_search(self, query_text: str, size: int = 10) -> List[Dict[str, Any]]:
        """
        Perform hybrid search combining vector similarity and text search
        
        Args:
            query_text: Text to search for
            size: Maximum number of results to return
            
        Returns:
            List of matching resume documents
        """
        # Return early for empty queries
        if not query_text or not query_text.strip():
            return []
            
        try:
            # Import standardized text function
            from src.utils.bedrock_embeddings import create_standardized_text
            
            # Process query text into structure
            query_structure = {
                "summary": query_text,
                "skills": [],
                "positions": [],
                "total_experience": 0,
                "companies": [],
                "education": [],
                "achievements": []
            }
            
            # Extract structured info if query is long enough
            if len(query_text) > 200:
                query_structure = self._extract_query_structure(query_text, query_structure)
            
            # Create standardized query text
            structured_query = create_standardized_text(query_structure)
            
            # Get embedding with caching
            query_embedding = self._get_embedding(structured_query, dimension=1024)
            
            # Log embedding dimension
            logger.info(f"Using query embedding with dimension {len(query_embedding)}")
            
            # Check if embedding is valid
            if all(x == 0 for x in query_embedding):
                logger.error("Error: Generated embedding is all zeros, falling back to text search")
                return self._text_search(query_text, size=size)
            
            # Extract key terms for better text matching
            key_terms = []
            if query_structure.get("skills"):
                key_terms.extend(query_structure["skills"])
            if query_structure.get("positions"):
                key_terms.extend(query_structure["positions"])
                
            # Build optimized hybrid query combining vector and text search
            search_query = {
                "size": size,
                "query": {
                    "bool": {
                        "should": [
                            # Vector search component with higher weight for semantic matching
                            {
                                "knn": {
                                    "resume_embedding": {
                                        "vector": query_embedding,
                                        "k": size,
                                        "boost": 3.0  # Higher weight for semantic matching
                                    }
                                }
                            },
                            # Text search components for keyword matching
                            {
                                "multi_match": {
                                    "query": query_text,
                                    "fields": [
                                        "skills^3",       # Higher weight for skills
                                        "positions^2.5",  # High weight for job titles
                                        "summary^1.5",    # Medium weight for summary
                                        "companies.description^1", 
                                        "projects.description^1",
                                        "education.degree^1"
                                    ],
                                    "type": "best_fields",
                                    "tie_breaker": 0.3,
                                    "fuzziness": "AUTO:4,7",
                                    "boost": 1.0
                                }
                            }
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "_source": {
                    "excludes": ["resume_embedding"]  # Don't return embeddings
                }
            }
            
            # Add boost for specific skills if available
            if key_terms and len(key_terms) > 0:
                term_queries = []
                for term in key_terms[:10]:  # Limit to top 10 terms
                    if len(term) >= 3:  # Skip very short terms
                        term_queries.append({
                            "match_phrase": {
                                "skills": {
                                    "query": term,
                                    "boost": 1.5  # Boost for specific skill matches
                                }
                            }
                        })
                
                # Add term queries if we have any valid ones
                if term_queries:
                    search_query["query"]["bool"]["should"].extend(term_queries)
            
            # Execute search with timeout
            response = self.opensearch.client.search(
                body=search_query,
                index=self.opensearch.index_name,
                request_timeout=15  # 15-second timeout for hybrid search
            )
            
            # Process results with improved normalization
            hits = response.get('hits', {}).get('hits', [])
            results = []
            
            if not hits:
                return []
                
            # Find min and max scores for better normalization
            scores = [hit.get('_score', 0) for hit in hits]
            max_score = max(scores) 
            min_score = min(scores)
            score_range = max(max_score - min_score, 0.0001)  # Avoid division by zero
            
            for hit in hits:
                doc = hit.get('_source', {})
                
                # Remove embedding to reduce payload size
                if 'resume_embedding' in doc:
                    del doc['resume_embedding']
                    
                # Normalize score with min-max scaling
                raw_score = hit.get('_score', 0)
                normalized_score = ((raw_score - min_score) / score_range) * 100
                
                # Apply sigmoid normalization for better distribution
                relevance_factor = 12.0  # Tune this parameter based on your data
                normalized_score = 100 * (1 / (1 + math.exp(-((normalized_score/100 - 0.5) * relevance_factor))))
                
                # Round to 2 decimal places and ensure 0-100 range
                doc['score'] = min(round(normalized_score, 2), 100)
                doc['raw_score'] = raw_score
                
                # Add vector score if available for debugging
                if 'fields' in hit and 'vector_score' in hit['fields']:
                    doc['vector_score'] = hit['fields']['vector_score'][0]
                
                results.append(doc)
            
            # Sort by score
            results.sort(key=lambda x: x['score'], reverse=True)
            
            logger.info(f"Hybrid search returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Error in hybrid search: {str(e)[:300]}")
            # Fall back to text search on failure
            try:
                return self._text_search(query_text, size=size)
            except:
                return []
    
    def _create_focused_search_query(self, job_description: str, jd_info: Dict[str, Any]) -> str:
        """
        Create a focused search query from job description for better vector search results
        
        Args:
            job_description: Original job description
            jd_info: Extracted JD information
            
        Returns:
            Focused query text optimized for vector search
        """
        # Create a focused query combining key elements from the JD
        query_parts = []
        
        # Add job title
        if jd_info.get('job_title'):
            query_parts.append(f"Job Title: {jd_info.get('job_title')}")
        
        # Add required skills
        if jd_info.get('required_skills'):
            query_parts.append(f"Required Skills: {', '.join(jd_info.get('required_skills'))}")
        
        # Add nice-to-have skills
        if jd_info.get('nice_to_have_skills'):
            query_parts.append(f"Nice-to-have Skills: {', '.join(jd_info.get('nice_to_have_skills'))}")
        
        # Add seniority level
        if jd_info.get('seniority_level'):
            query_parts.append(f"Seniority Level: {jd_info.get('seniority_level')}")
        
        # Add industry
        if jd_info.get('industry'):
            query_parts.append(f"Industry: {jd_info.get('industry')}")
        
        # If we have enough extracted info, use the focused query
        if len(query_parts) >= 3:
            return "\n".join(query_parts)
        
        # Fallback to using the original job description
        return job_description
    
    def get_resume_details(self, resume_id: str) -> Dict[str, Any]:
        """
        Get complete resume details from all data sources
        
        Args:
            resume_id: Resume ID
            
        Returns:
            Combined resume data from all sources
        """
        # Get PII data from PostgreSQL
        pii_data = None
        try:
            with self.postgres:
                pii_data = self.postgres.get_resume_pii(resume_id)
        except Exception as e:
            logger.warning(f"Error retrieving PII data for resume {resume_id}: {str(e)}")
        
        # Get structured data from DynamoDB
        ddb_data = None
        try:
            ddb_data = self.dynamodb.get_resume(resume_id)
        except Exception as e:
            logger.warning(f"Error retrieving DynamoDB data for resume {resume_id}: {str(e)}")
        
        # Combine data
        combined_data = {
            "resume_id": resume_id,
            "pii": pii_data or {},
            "data": ddb_data or {}
        }
        
        return combined_data
    
    def normalize_skill(self, skill: str) -> str:
        """
        Normalize skill names to handle variations and abbreviations
        
        Args:
            skill: Original skill name
            
        Returns:
            Normalized skill name
        """
        # Convert to lowercase and trim whitespace
        skill = skill.lower().strip()
        
        # Remove common prefixes/suffixes and punctuation
        skill = skill.replace(".", "").replace("-", " ").replace("/", " ")
        skill = skill.replace("(", "").replace(")", "").replace(",", "")
        
        # Common abbreviations and variations
        SKILL_MAP = {
            # Programming languages
            "js": "javascript",
            "ts": "typescript",
            "py": "python",
            "java8": "java",
            "java 8": "java",
            "java11": "java",
            "java 11": "java",
            "java17": "java",
            "java 17": "java",
            "core java": "java",
            "java core": "java",
            "java se": "java",
            "java ee": "java",
            "c#": "csharp",
            "c++": "cplusplus",
            "c/c++": "cplusplus",
            "objective c": "objectivec",
            "objective-c": "objectivec",
            
            # Frameworks and libraries
            "react.js": "react",
            "reactjs": "react",
            "react js": "react",
            "vue.js": "vue",
            "vuejs": "vue",
            "vue js": "vue",
            "node.js": "node",
            "nodejs": "node",
            "node js": "node",
            "angular.js": "angular",
            "angularjs": "angular",
            "angular js": "angular",
            "spring boot": "spring",
            "springboot": "spring",
            "spring framework": "spring",
            "django framework": "django",
            "flask framework": "flask",
            "express.js": "express",
            "expressjs": "express",
            "express js": "express",
            
            # Cloud and DevOps
            "aws": "amazon web services",
            "amazon aws": "amazon web services",
            "ec2": "aws ec2",
            "s3": "aws s3",
            "gcp": "google cloud platform",
            "azure": "microsoft azure",
            "k8s": "kubernetes",
            "ci/cd": "ci cd",
            "cicd": "ci cd",
            "ci-cd": "ci cd",
            "devops": "dev ops",
            "docker container": "docker",
            "containerization": "containers",
            
            # Data science and ML
            "ml": "machine learning",
            "ai": "artificial intelligence",
            "nlp": "natural language processing",
            "cv": "computer vision",
            "dl": "deep learning",
            "tf": "tensorflow",
            "pt": "pytorch",
            
            # Databases
            "sql server": "microsoft sql server",
            "mssql": "microsoft sql server",
            "sqlserver": "microsoft sql server",
            "postgres": "postgresql",
            "postgre": "postgresql",
            "mongo": "mongodb",
            "cosmosdb": "azure cosmos db",
            "dynamodb": "aws dynamodb",
            "rds": "aws rds",
            "mysql db": "mysql",
            "oracle db": "oracle",
            
            # Web development
            "html5": "html",
            "css3": "css",
            "scss": "css",
            "sass": "css",
            "less": "css",
            "restful": "rest",
            "rest api": "rest apis",
            "restful api": "rest apis",
            "restful apis": "rest apis",
            "restful services": "rest apis",
            "rest services": "rest apis",
            "restful web services": "rest apis",
            "web services": "apis",
            "microservice": "microservices",
            "micro service": "microservices",
            "micro-service": "microservices",
            "micro services": "microservices",
            "micro-services": "microservices",
            
            # Testing
            "junit testing": "junit",
            "unit testing": "unit tests",
            "integration testing": "integration tests",
            "test automation": "automated testing",
            
            # Version control
            "git hub": "github",
            "gitlab": "git",
            "bitbucket": "git",
            "svn": "subversion",
            
            # Project management
            "agile methodology": "agile",
            "agile methodologies": "agile",
            "scrum methodology": "scrum",
            "kanban methodology": "kanban",
            "jira software": "jira",
            
            # Security
            "oauth2": "oauth",
            "oauth 2.0": "oauth",
            "openid connect": "openid",
            "oidc": "openid",
            "jsonwebtoken": "jwt",
            "authentication": "auth",
            "authorization": "auth"
        }
        
        # Return mapped skill or original if no mapping exists
        return SKILL_MAP.get(skill, skill)
    
    def calculate_skill_match_score(self, resume_skills: List[str], jd_skills: List[str]) -> float:
        """
        Calculate skill match score between resume and job description
        
        Args:
            resume_skills: List of skills from resume
            jd_skills: List of skills extracted from job description
            
        Returns:
            Skill match score (0-100)
        """
        if not resume_skills or not jd_skills:
            return 0.0
        
        # Normalize skills (lowercase and handle variations)
        resume_skills_norm = [self.normalize_skill(skill) for skill in resume_skills]
        jd_skills_norm = [self.normalize_skill(skill) for skill in jd_skills]
        
        # Count exact matches using normalized skills
        exact_matches = sum(1 for skill in jd_skills_norm if skill in resume_skills_norm)
        
        # Calculate partial matches with improved logic
        partial_matches = 0
        for jd_skill in jd_skills_norm:
            if jd_skill not in resume_skills_norm:
                # Check for substring matches (both directions)
                for resume_skill in resume_skills_norm:
                    # Only consider meaningful substrings (at least 4 chars)
                    if len(jd_skill) >= 4 and len(resume_skill) >= 4:
                        # Check if JD skill is part of resume skill
                        if jd_skill in resume_skill:
                            partial_matches += 0.75  # Higher weight for substring match
                            break
                        # Check if resume skill is part of JD skill
                        elif resume_skill in jd_skill:
                            partial_matches += 0.5  # Medium weight for this case
                            break
                        # Check for significant word overlap in multi-word skills
                        elif ' ' in jd_skill and ' ' in resume_skill:
                            jd_words = set(jd_skill.split())
                            resume_words = set(resume_skill.split())
                            common_words = jd_words.intersection(resume_words)
                            if len(common_words) >= 2 or (len(common_words) == 1 and len(jd_words) <= 2):
                                partial_matches += 0.5  # Medium weight for word overlap
                                break
        
        # Calculate total score with partial matches
        total_matches = exact_matches + partial_matches
        
        # Calculate score - weight exact matches more heavily
        # Formula: (exact_matches * 1.0 + partial_matches * 0.5) / total_skills * 100
        weighted_score = (exact_matches + partial_matches) / len(jd_skills_norm) * 100 if jd_skills_norm else 0
        
        # Bonus for high coverage of critical skills
        if jd_skills_norm and exact_matches >= len(jd_skills_norm) * 0.7:
            weighted_score *= 1.15  # 15% bonus for covering 70%+ of required skills
            weighted_score = min(weighted_score, 100)  # Cap at 100
        
        return round(weighted_score, 2)
    
    def extract_skills_from_jd(self, job_description: str) -> List[str]:
        """
        Extract skills from job description using LLM with pattern matching as fallback
        
        Args:
            job_description: Job description text
            
        Returns:
            List of skills
        """
        try:
            # Try LLM-based extraction first
            skills = self._extract_skills_llm(job_description)
            if skills:
                logger.info(f"Successfully extracted {len(skills)} skills using LLM")
                return skills
        except Exception as e:
            logger.error(f"LLM skill extraction failed: {str(e)}")
        
        # Fall back to pattern matching
        logger.info("Falling back to pattern matching for skill extraction")
        return self._extract_skills_pattern_matching(job_description)
    
    def _extract_skills_llm(self, job_description: str) -> List[str]:
        """
        Extract skills from job description using LLM
        
        Uses MODEL_ID (via BEDROCK_MODEL_ID) for LLM-based analysis, NOT the embedding model.
        
        Args:
            job_description: Job description text
            
        Returns:
            List of skills
        """
        from config.config import BEDROCK_MODEL_ID, AWS_REGION
        
        # Verify we have an LLM model to use
        if not BEDROCK_MODEL_ID:
            logger.warning("No MODEL_ID provided, falling back to pattern matching")
            return self._extract_skills_pattern_matching(job_description)
        
        # Initialize Bedrock client
        bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            region_name=AWS_REGION
        )
        
        # Craft prompt for skill extraction
        prompt = f"""You are a skilled technical recruiter with expertise in identifying technical skills from job descriptions.
        
        Extract all technical skills, tools, technologies, frameworks, programming languages, and domain knowledge 
        from the following job description. Return ONLY a JSON array of strings containing the skills.
        Only include specific skills, not general concepts or responsibilities.
        
        Job Description:
        {job_description}
        """
        
        # Call Bedrock model with model-specific parameters
        # Adapt request format by model
        if "claude" in BEDROCK_MODEL_ID.lower():
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "temperature": 0.1,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
        elif "titan" in BEDROCK_MODEL_ID.lower():
            request_body = {
                "inputText": prompt,
                "textGenerationConfig": {
                    "maxTokenCount": 1000,
                    "temperature": 0.1
                }
            }
        elif "llama" in BEDROCK_MODEL_ID.lower():
            # Llama models use a different parameter format
            request_body = {
                "prompt": prompt,
                "max_gen_len": 1000,
                "temperature": 0.1
            }
        else:
            # Default format for other models
            request_body = {
                "prompt": prompt,
                "max_tokens_to_sample": 1000,
                "temperature": 0.1
            }
            
        # Call Bedrock LLM model
        try:
            logger.info(f"Calling Bedrock LLM model (MODEL_ID): {BEDROCK_MODEL_ID} for skill extraction")
            response = bedrock_runtime.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                body=json.dumps(request_body)
            )
            
            # Parse response based on model type
            response_body = json.loads(response.get("body").read())
            completion = ""
            
            if "claude" in BEDROCK_MODEL_ID.lower() and "content" in response_body:
                completion = response_body.get("content", [{}])[0].get("text", "")
            elif "llama" in BEDROCK_MODEL_ID.lower():
                # Llama models return 'generation' field
                completion = response_body.get("generation", "")
            elif "titan" in BEDROCK_MODEL_ID.lower():
                completion = response_body.get("results", [{}])[0].get("outputText", "")
            else:
                # Claude models return 'completion' field
                completion = response_body.get("completion", "")
            
            logger.debug(f"LLM response: {completion[:100]}...")
            
            # Extract JSON array from completion
            import re
            skills_match = re.search(r'\[(.*?)\]', completion, re.DOTALL)
            if skills_match:
                skills_json = f"[{skills_match.group(1)}]"
                try:
                    # Clean up the JSON string (remove extra whitespace, fix quotes)
                    skills_json = skills_json.replace("'", '"')
                    # Handle potential trailing commas which are invalid in JSON
                    skills_json = re.sub(r',\s*]', ']', skills_json)
                    skills = json.loads(skills_json)
                    return [skill.strip() for skill in skills if skill.strip()]
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse skills JSON from LLM response: {str(e)}")
                    logger.debug(f"Problematic JSON: {skills_json}")
            else:
                logger.warning("Could not find JSON array in LLM response")
                
            # Try a more aggressive approach to extract any array-like structure
            any_skills = re.findall(r'"([^"]+)"', completion)
            if any_skills:
                logger.info(f"Extracted {len(any_skills)} skills using regex fallback")
                return [skill.strip() for skill in any_skills if skill.strip()]
                
        except Exception as e:
            logger.error(f"Error calling Bedrock model: {str(e)}")
        
        # If we couldn't extract skills, return empty list
        return []
    
    def _extract_skills_pattern_matching(self, job_description: str) -> List[str]:
        """
        Extract skills from job description using pattern matching
        
        Args:
            job_description: Job description text
            
        Returns:
            List of skills
        """
        # Simple skill extraction based on common tech terms
        # In a real implementation, you might use NLP or a more sophisticated approach
        common_skills = [
            "python", "java", "javascript", "react", "angular", "node", "aws",
            "azure", "gcp", "docker", "kubernetes", "sql", "nosql", "mongodb",
            "postgresql", "mysql", "oracle", "rest", "api", "microservices",
            "ci/cd", "devops", "agile", "scrum", "git", "machine learning", "ai",
            "data science", "big data", "hadoop", "spark", "tableau", "power bi",
            "excel", "word", "powerpoint", "jira", "confluence", "linux", "unix",
            "windows", "c#", "c++", "ruby", "php", "html", "css", "sass", "less",
            "typescript", "vue", "redux", "graphql", "django", "flask", "spring",
            "hibernate", "jenkins", "terraform", "ansible", "puppet", "chef",
            "blockchain", "ethereum", "solidity", "ios", "android", "swift",
            "kotlin", "react native", "flutter", "xamarin", "unity", "unreal",
            "sap", "salesforce", "dynamics", "sharepoint", "azure devops",
            "aws lambda", "serverless", "kafka", "rabbitmq", "redis", "elasticsearch",
            "kibana", "logstash", "grafana", "prometheus", "datadog", "new relic",
            "splunk", "sumo logic", "nginx", "apache", "tomcat", "iis", "weblogic",
            "websphere", "jboss", "wildfly", "maven", "gradle", "npm", "yarn",
            "webpack", "babel", "jest", "mocha", "cypress", "selenium", "appium",
            "junit", "testng", "nunit", "xunit", "pytest", "rspec", "cucumber",
            "gherkin", "bdd", "tdd", "agile", "scrum", "kanban", "lean", "six sigma",
            "itil", "cobit", "togaf", "prince2", "pmp", "capm", "csm", "safe",
            "cisa", "cissp", "ceh", "comptia", "aws certified", "azure certified",
            "gcp certified", "pci dss", "hipaa", "gdpr", "sox", "iso 27001",
            "nist", "fedramp", "hitrust", "cmmi", "iso 9001", "iso 14001",
            "iso 45001", "iso 13485", "fda", "21 cfr part 11", "gxp", "gmp",
            "glp", "gcp", "gdp", "gtp", "gvp", "gpp", "gep", "gcp", "gsp",
            "gstp", "gamp", "ispe", "ich", "emea", "fda", "mhra", "pmda",
            "anvisa", "cofepris", "tga", "hsa", "nmpa", "cdsco", "kfda",
            "sfda", "tfda", "dgda", "nafdac", "sahpra", "who", "ema",
            "edqm", "usp", "bp", "jp", "ph eur", "usp-nf", "jp-nf"
        ]
        
        # Extract skills from job description
        jd_lower = job_description.lower()
        found_skills = []
        
        for skill in common_skills:
            if skill in jd_lower:
                found_skills.append(skill)
        
        return found_skills
    
    def calculate_experience_match(self, resume_exp: float, jd_required_exp: float) -> float:
        """
        Calculate experience match score
        
        Args:
            resume_exp: Years of experience in resume
            jd_required_exp: Years of experience required in job description
            
        Returns:
            Experience match score (0-100)
        """
        if resume_exp >= jd_required_exp:
            return 100.0
        
        # Partial match
        return round((resume_exp / jd_required_exp) * 100, 2) if jd_required_exp > 0 else 0
    
    def calculate_overall_ranking(self, resume_data: Dict[str, Any], jd_skills: List[str], jd_required_exp: float, search_score: float) -> Dict[str, Any]:
        """
        Calculate overall ranking for a resume
        
        Args:
            resume_data: Resume data
            jd_skills: Skills extracted from job description
            jd_required_exp: Years of experience required
            search_score: Search score from OpenSearch
            
        Returns:
            Ranking data
        """
        # Extract resume skills
        resume_skills = []
        if 'data' in resume_data and resume_data['data']:
            if 'skills' in resume_data['data']:
                if isinstance(resume_data['data']['skills'], list):
                    resume_skills = resume_data['data']['skills']
                elif isinstance(resume_data['data']['skills'], str):
                    resume_skills = [resume_data['data']['skills']]
        
        # Extract years of experience - check both possible field names
        years_exp = 0
        if 'data' in resume_data and resume_data['data']:
            # Try total_years_experience first (new field name)
            if 'total_years_experience' in resume_data['data']:
                try:
                    years_exp = float(resume_data['data']['total_years_experience'])
                except (ValueError, TypeError):
                    pass
            # Try total_experience (field name in existing data)
            elif 'total_experience' in resume_data['data']:
                try:
                    years_exp = float(resume_data['data']['total_experience'])
                except (ValueError, TypeError):
                    pass
            
            # Log the extracted experience
            logger.info(f"Extracted {years_exp} years of experience for resume {resume_data.get('resume_id')}")
        
        # Calculate individual scores
        skill_score = self.calculate_skill_match_score(resume_skills, jd_skills)
        exp_score = self.calculate_experience_match(years_exp, jd_required_exp)
        
        # Search score should already be normalized by the OpenSearch handler (0-100)
        # Just ensure it's in the correct range
        normalized_search_score = min(max(search_score, 0), 100)
        
        # Check if candidate meets minimum experience requirement
        meets_min_exp = years_exp >= jd_required_exp
        
        # Apply experience requirement penalty if needed
        # If candidate doesn't meet minimum experience, reduce their overall score
        exp_penalty = 1.0 if meets_min_exp else 0.7
        
        # Calculate overall score with improved weights
        # Vector score: 40%, Skills: 30%, Experience: 20%, Exp penalty: applied to total
        combined_score = (
            normalized_search_score * 0.40 +  # 40% weight to vector score (down from 60%)
            skill_score * 0.30 +              # 30% weight to skill match (up from 25%)
            exp_score * 0.30                  # 30% weight to experience match (up from 15%)
        ) * exp_penalty
        
        # Create ranking data with field names matching what's used in process_job_description
        ranking_data = {
            "resume_id": resume_data.get('resume_id', ''),
            "resume_data": resume_data.get('data', {}),
            "pii_data": resume_data.get('pii', {}),
            "search_score": round(normalized_search_score, 2),
            "combined_score": round(combined_score, 2),
            "skill_match_score": skill_score,
            "experience_match_score": exp_score,
            "meets_min_experience": meets_min_exp,
            "matched_skills": [skill for skill in jd_skills if skill.lower() in [s.lower() for s in resume_skills]],
            "missing_skills": [skill for skill in jd_skills if skill.lower() not in [s.lower() for s in resume_skills]],
            "years_experience": years_exp,
            "required_experience": jd_required_exp
        }
        
        return ranking_data
    
    def extract_jd_info_llm(self, job_description: str) -> Dict[str, Any]:
        """
        Extract comprehensive information from job description using LLM
        
        Uses MODEL_ID (via BEDROCK_MODEL_ID) for LLM-based analysis, NOT the embedding model.
        
        Args:
            job_description: Job description text
            
        Returns:
            Dictionary with extracted information
        """
        from config.config import BEDROCK_MODEL_ID, AWS_REGION
        
        # Verify we have an LLM model to use
        if not BEDROCK_MODEL_ID:
            logger.warning("No MODEL_ID provided, falling back to default extraction")
            return {
                "job_title": "Not specified",
                "required_experience": 0.0,
                "required_skills": self._extract_skills_pattern_matching(job_description),
                "nice_to_have_skills": []
            }
        
        # Initialize Bedrock client
        bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            region_name=AWS_REGION
        )
        
        # Craft prompt for JD information extraction
        prompt = f"""You are an expert job description analyzer. Extract the following information from the job description below:

1. Job Title
2. Required Years of Experience (as a number only)
3. Required Skills (as a list)
4. Nice-to-Have Skills (as a list)
5. Seniority Level (e.g., Junior, Mid-level, Senior, Lead)
6. Job Type (e.g., Full-time, Contract, Remote)
7. Industry
8. Required Education (e.g., Bachelor's, Master's)

Return ONLY a valid JSON object with the following format (no other text):
{{
  "job_title": "string",
  "required_experience": number,
  "required_skills": ["skill1", "skill2"],
  "nice_to_have_skills": ["skill1", "skill2"],
  "seniority_level": "string",
  "job_type": "string",
  "industry": "string",
  "required_education": "string"
}}

Make sure:
- All keys are double-quoted
- required_experience is a number (not a string)
- All arrays use square brackets
- No trailing commas
- All strings are properly quoted with double quotes

Job Description:
{job_description}
"""
        
        try:
            # Prepare request body based on model type
            if "claude" in BEDROCK_MODEL_ID.lower():
                request_body = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 1000,
                    "temperature": 0.1,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                }
            elif "titan" in BEDROCK_MODEL_ID.lower():
                request_body = {
                    "inputText": prompt,
                    "textGenerationConfig": {
                        "maxTokenCount": 1000,
                        "temperature": 0.1
                    }
                }
            elif "llama" in BEDROCK_MODEL_ID.lower():
                # Llama models use a different parameter format
                request_body = {
                    "prompt": prompt,
                    "max_gen_len": 1000,
                    "temperature": 0.1
                }
            else:
                # Default format for other models
                request_body = {
                    "prompt": prompt,
                    "max_tokens_to_sample": 1000,
                    "temperature": 0.1
                }
            
            # Call Bedrock LLM model
            logger.info(f"Calling Bedrock LLM model (MODEL_ID): {BEDROCK_MODEL_ID} for JD analysis")
            response = bedrock_runtime.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                body=json.dumps(request_body)
            )
            
            # Parse response based on model type
            response_body = json.loads(response.get("body").read())
            completion = ""
            
            if "claude" in BEDROCK_MODEL_ID.lower() and "content" in response_body:
                completion = response_body.get("content", [{}])[0].get("text", "")
            elif "llama" in BEDROCK_MODEL_ID.lower():
                # Llama models return 'generation' field
                completion = response_body.get("generation", "")
            elif "titan" in BEDROCK_MODEL_ID.lower():
                completion = response_body.get("results", [{}])[0].get("outputText", "")
            else:
                # Default to completion field
                completion = response_body.get("completion", "")
            
            # Extract JSON from the completion
            import re
            json_match = re.search(r'\{[\s\S]*\}', completion, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(0)
                try:
                    jd_info = json.loads(json_str)
                    logger.info(f"Successfully extracted JD info using LLM")
                    
                    # Ensure key fields are present
                    if 'required_experience' not in jd_info or not jd_info['required_experience']:
                        experience_match = re.search(r'(\d+)(?:\s*[-+]?\s*\d*)?\s+years?\s+(?:of\s+)?experience', job_description, re.IGNORECASE)
                        if experience_match:
                            jd_info['required_experience'] = int(experience_match.group(1))
                        else:
                            jd_info['required_experience'] = 0
                            
                    # If no skills found, try pattern matching
                    if 'required_skills' not in jd_info or not jd_info['required_skills']:
                        jd_info['required_skills'] = self._extract_skills_pattern_matching(job_description)
                        
                    # If no job title found, try to extract it
                    if 'job_title' not in jd_info or not jd_info['job_title']:
                        title_match = re.search(r'^([^.:\n]{5,100})', job_description)
                        if title_match:
                            jd_info['job_title'] = title_match.group(1).strip()
                        else:
                            jd_info['job_title'] = "Not specified"
                    
                    return jd_info
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JD info JSON from LLM response: {str(e)}")
            else:
                logger.warning("Could not find JSON object in LLM response")
                
        except Exception as e:
            logger.error(f"Error extracting JD info using LLM: {str(e)}")
        
        # Fallback to regex-based extraction
        logger.info("Using fallback regex-based JD analysis")
        default_info = {
            "job_title": "Not specified",
            "required_experience": 0,
            "required_skills": self._extract_skills_pattern_matching(job_description),
            "nice_to_have_skills": []
        }
        
        # Extract job title using regex
        title_match = re.search(r'^([^.:\n]{5,100})', job_description)
        if title_match:
            default_info['job_title'] = title_match.group(1).strip()
        
        # Extract required experience using regex
        experience_match = re.search(r'(\d+)(?:\s*[-+]?\s*\d*)?\s+years?\s+(?:of\s+)?experience', job_description, re.IGNORECASE)
        if experience_match:
            default_info['required_experience'] = int(experience_match.group(1))
        
        return default_info
    
    def analyze_resume_match_llm(self, resume_data: Dict[str, Any], job_description: str) -> Dict[str, Any]:
        """
        Use LLM to analyze how well a resume matches a job description
        
        Args:
            resume_data: Resume data
            job_description: Job description text
            
        Returns:
            Analysis of the match
        """
        from config.config import BEDROCK_MODEL_ID, AWS_REGION
        
        # Extract relevant resume information for the prompt
        resume_text = ""
        
        # Add basic information
        if 'pii' in resume_data and resume_data['pii']:
            if 'name' in resume_data['pii']:
                resume_text += f"Name: {resume_data['pii'].get('name', 'Not specified')}\n"
            if 'email' in resume_data['pii']:
                resume_text += f"Email: {resume_data['pii'].get('email', 'Not specified')}\n"
            if 'phone' in resume_data['pii']:
                resume_text += f"Phone: {resume_data['pii'].get('phone', 'Not specified')}\n"
            if 'location' in resume_data['pii']:
                resume_text += f"Location: {resume_data['pii'].get('location', 'Not specified')}\n"
        
        # Add skills
        if 'data' in resume_data and resume_data['data']:
            if 'skills' in resume_data['data']:
                if isinstance(resume_data['data']['skills'], list):
                    resume_text += f"Skills: {', '.join(resume_data['data']['skills'])}\n"
                elif isinstance(resume_data['data']['skills'], str):
                    resume_text += f"Skills: {resume_data['data']['skills']}\n"
            
            # Add experience
            if 'total_years_experience' in resume_data['data']:
                resume_text += f"Total Experience: {resume_data['data'].get('total_years_experience')} years\n"
            elif 'total_experience' in resume_data['data']:
                resume_text += f"Total Experience: {resume_data['data'].get('total_experience')} years\n"
            
            # Add work history
            if 'work_history' in resume_data['data'] and resume_data['data']['work_history']:
                resume_text += "Work History:\n"
                for job in resume_data['data']['work_history']:
                    company = job.get('company', 'Unknown Company')
                    title = job.get('title', 'Unknown Title')
                    start_date = job.get('start_date', 'Unknown')
                    end_date = job.get('end_date', 'Present')
                    description = job.get('description', '')
                    resume_text += f"- {title} at {company} ({start_date} - {end_date})\n"
                    if description:
                        resume_text += f"  Description: {description}\n"
            
            # Add education
            if 'education' in resume_data['data'] and resume_data['data']['education']:
                resume_text += "Education:\n"
                for edu in resume_data['data']['education']:
                    degree = edu.get('degree', 'Unknown Degree')
                    institution = edu.get('institution', 'Unknown Institution')
                    resume_text += f"- {degree} from {institution}\n"
        
        # Initialize Bedrock client
        bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            region_name=AWS_REGION
        )
        
        # Craft prompt for resume match analysis
        prompt = f"""You are an expert recruiter. Analyze how well the following resume matches the job description.

Job Description:
{job_description}

Resume:
{resume_text}

Provide your analysis as a JSON object with the following format:
{{
  "match_score": number,  // A score from 0-100 indicating overall match
  "strengths": ["string", "string"],  // List of candidate's strengths for this role
  "gaps": ["string", "string"],  // List of areas where the candidate may be lacking
  "recommendation": "string",  // Your recommendation (Highly Recommend, Recommend, Maybe, Not Recommended)
  "explanation": "string"  // Brief explanation of your recommendation
}}

Return ONLY the JSON object, no other text.
"""
        
        try:
            # Call Bedrock model with model-specific parameters
            if "llama" in BEDROCK_MODEL_ID.lower():
                # Llama models use a different parameter format
                request_body = {
                    "prompt": prompt,
                    "max_gen_len": 1000,
                    "temperature": 0.1
                }
            else:
                # Default format for other models like Claude
                request_body = {
                    "prompt": prompt,
                    "max_tokens_to_sample": 1000,
                    "temperature": 0.1
                }
            
            # Call Bedrock model
            response = bedrock_runtime.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                body=json.dumps(request_body)
            )
            
            # Parse response based on model type
            response_body = json.loads(response.get("body").read())
            
            if "llama" in BEDROCK_MODEL_ID.lower():
                # Llama models return 'generation' field
                completion = response_body.get("generation", "")
            else:
                # Claude models return 'completion' field
                completion = response_body.get("completion", "")
            
            # Extract JSON from the completion
            import re
            json_match = re.search(r'\{[\s\S]*\}', completion, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(0)
                try:
                    analysis = json.loads(json_str)
                    logger.info(f"Successfully analyzed resume match using LLM")
                    return analysis
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse resume analysis JSON from LLM response: {str(e)}")
            else:
                logger.warning("Could not find JSON object in LLM response")
                
        except Exception as e:
            logger.error(f"Error analyzing resume match using LLM: {str(e)}")
        
        # Default return if LLM analysis fails
        return {
            "match_score": 0,
            "strengths": [],
            "gaps": [],
            "recommendation": "Not Available",
            "explanation": "LLM analysis failed"
        }

    def process_job_description(self, job_description: str, max_results: int = 20, enable_reranking: bool = True, search_method: str = None) -> str:
        """
        Process a job description to find matching resumes and generate a report
        
        Args:
            job_description: Job description text
            max_results: Maximum number of results to return
            enable_reranking: Whether to apply reranking
            search_method: Ignored parameter (kept for backward compatibility)
            
        Returns:
            Path to the generated report file
        """
        logger.info(f"Processing job description with hybrid search")
        
        try:
            # Start timing
            start_time = datetime.now()
            
            # Generate unique ID for this search
            search_id = str(uuid.uuid4())[:8]
            
            # Analyze the JD to extract requirements
            jd_info = self.extract_jd_info_llm(job_description)
            required_experience = jd_info.get('required_experience', 0)
            required_skills = jd_info.get('required_skills', [])
            job_title = jd_info.get('job_title', 'Not specified')
            
            # Log extracted info
            logger.info(f"Job Title: {job_title}")
            logger.info(f"Required Experience: {required_experience} years")
            logger.info(f"Required Skills: {', '.join(required_skills) if required_skills else 'None specified'}")
            
            # Search for matching resumes - pass jd_info to avoid duplicate LLM calls
            results = self.search_resumes_by_jd(
                job_description, 
                max_results=max_results,
                jd_info=jd_info,
                enable_reranking=enable_reranking
            )
            
            # End timing
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Create report content
            report_content = {
                "timestamp": datetime.now().isoformat(),
                "search_id": search_id,
                "search_method": "hybrid",
                "job_title": job_title,
                "required_experience": required_experience,
                "required_skills": required_skills,
                "duration_seconds": round(duration, 2),
                "total_results": len(results),
                "results": results
            }
            
            # Create report filename
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            job_title_slug = job_title.lower().replace(" ", "-")[:30]
            filename = f"jd-matches_{job_title_slug}_hybrid_{timestamp}.json"
            
            # Create report file path
            report_path = os.path.join(self.output_dir, filename)
            
            # Write report to file
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report_content, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Generated report at: {report_path}")
            logger.info(f"Found {len(results)} matching resumes in {round(duration, 2)} seconds")
            
            return report_path
            
        except Exception as e:
            logger.error(f"Error processing job description: {str(e)}")
            # Return empty string on error
            return ""

    def process_job_description_file(self, jd_file_path: str, max_results: int = 20, enable_reranking: bool = True, search_method: str = None) -> str:
        """
        Process job description from a file
        
        Args:
            jd_file_path: Path to job description file
            max_results: Maximum number of results to return
            enable_reranking: Whether to apply reranking
            search_method: Ignored parameter (kept for backward compatibility)
            
        Returns:
            Path to the generated report file
        """
        try:
            # Read job description from file
            with open(jd_file_path, 'r', encoding='utf-8') as f:
                job_description = f.read()
                
            logger.info(f"Read job description from {jd_file_path} ({len(job_description)} characters)")
            
            # Process job description
            return self.process_job_description(
                job_description, 
                max_results=max_results,
                enable_reranking=enable_reranking
            )
            
        except Exception as e:
            logger.error(f"Error processing job description file: {str(e)}")
            return ""
    
    def process_multiple_job_descriptions(self, jd_dir: str, max_results: int = 20, enable_reranking: bool = True, search_method: str = None) -> List[str]:
        """
        Process multiple job descriptions from a directory
        
        Args:
            jd_dir: Path to directory containing job description files
            max_results: Maximum number of results to return
            enable_reranking: Whether to apply reranking
            search_method: Ignored parameter (kept for backward compatibility)
            
        Returns:
            List of paths to the generated report files
        """
        # Check if directory exists
        if not os.path.isdir(jd_dir):
            logger.error(f"Directory does not exist: {jd_dir}")
            return []
            
        # Get job description files
        jd_files = [f for f in os.listdir(jd_dir) if f.endswith(('.txt', '.md', '.json'))]
        
        if not jd_files:
            logger.error(f"No job description files found in {jd_dir}")
            return []
            
        logger.info(f"Found {len(jd_files)} job description files")
        
        # Process each file
        output_paths = []
        for jd_file in jd_files:
            jd_path = os.path.join(jd_dir, jd_file)
            logger.info(f"Processing {jd_path}")
            
            output_path = self.process_job_description_file(
                jd_path, 
                max_results=max_results,
                enable_reranking=enable_reranking
            )
            
            if output_path:
                output_paths.append(output_path)
        
        return output_paths

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Resume retrieval based on job description")
    
    # Input method options - file, directory or text
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--jd", type=str, help="Job description text")
    input_group.add_argument("--jd-file", type=str, help="Path to job description file")
    input_group.add_argument("--jd-dir", type=str, help="Path to job description directory")
    
    # Search method (kept for backward compatibility but ignored)
    parser.add_argument("--method", type=str, choices=["text", "vector", "hybrid"], default="hybrid", 
                        help="Search method (always uses hybrid search)")
    
    # Results options
    parser.add_argument("--max-results", type=int, default=10, help="Maximum number of results")
    
    # Vector search options
    parser.add_argument("--disable-reranking", action="store_true", help="Disable reranking in vector search")
    
    return parser.parse_args()

def main():
    """Main function"""
    # Parse command line arguments
    args = parse_arguments()
    
    # Initialize retriever
    retriever = ResumeRetriever()
    
    # Determine enable_reranking value
    enable_reranking = not args.disable_reranking
    
    # Process job description based on input method
    if args.jd:
        # Process job description text
        output_path = retriever.process_job_description(
            job_description=args.jd, 
            search_method=args.method, 
            max_results=args.max_results,
            enable_reranking=enable_reranking
        )
        
        # Print output path
        if output_path:
            print(f"\nResults saved to: {output_path}")
        else:
            print("\nFailed to process job description")
            
    elif args.jd_file:
        # Process job description file
        output_path = retriever.process_job_description_file(
            jd_file_path=args.jd_file, 
            search_method=args.method, 
            max_results=args.max_results,
            enable_reranking=enable_reranking
        )
        
        # Print output path
        if output_path:
            print(f"\nResults saved to: {output_path}")
        else:
            print("\nFailed to process job description file")
            
    elif args.jd_dir:
        # Process multiple job description files
        output_paths = retriever.process_multiple_job_descriptions(
            jd_dir=args.jd_dir, 
            search_method=args.method, 
            max_results=args.max_results,
            enable_reranking=enable_reranking
        )
        
        # Print output paths
        if output_paths:
            print(f"\nProcessed {len(output_paths)} job descriptions")
            print("Results saved to:")
            for path in output_paths:
                print(f"  {path}")
        else:
            print("\nFailed to process job description files")
            
if __name__ == "__main__":
    main() 