import json
import logging
import re
import uuid
import time
import hashlib
from typing import Dict, Any, Optional, Tuple

from config.config import BEDROCK_MAX_INPUT_TOKENS, BEDROCK_CHAR_PER_TOKEN, ENABLE_OPENSEARCH

logger = logging.getLogger(__name__)

# Global cache for resume processing results
_GLOBAL_CACHE = {}
_CACHE_MAX_SIZE = 200

# Global clients
_bedrock_client = None
_opensearch_handler = None

def get_text_hash(text):
    """Generate a stable hash for text content"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

class ResumeExtractor:
    """Extract structured information from resume text using AWS Bedrock"""
    
    def __init__(self, model_id: Optional[str] = None):
        """
        Initialize resume extractor with Bedrock client
        
        Args:
            model_id: AWS Bedrock model ID
        """
        self.model_id = model_id
        # Don't initialize clients here - lazy load when needed
        self._bedrock_client = None
        self._opensearch_handler = None
        logger.info("Initialized ResumeExtractor (clients will be loaded when needed)")
    
    @property
    def bedrock_client(self):
        """Lazy load the Bedrock client"""
        global _bedrock_client
        if _bedrock_client is None:
            from src.utils.bedrock_client import BedrockClient
            _bedrock_client = BedrockClient(model_id=self.model_id)
            logger.info("Initialized Bedrock client")
        return _bedrock_client
    
    @property
    def opensearch_handler(self):
        """Lazy load the OpenSearch handler"""
        global _opensearch_handler
        if ENABLE_OPENSEARCH and _opensearch_handler is None:
            from src.storage.opensearch_handler import OpenSearchHandler
            try:
                _opensearch_handler = OpenSearchHandler()
                logger.info("Initialized OpenSearch handler")
            except Exception as e:
                logger.error(f"Failed to initialize OpenSearch handler: {str(e)}")
                _opensearch_handler = None
        return _opensearch_handler
    
    def process_resume(self, resume_text: str) -> Tuple[Dict[str, Any], str]:
        """
        Process resume text and extract structured information
        
        Args:
            resume_text: Resume text content
            
        Returns:
            Tuple of (structured resume data, resume_id)
        """
        global _GLOBAL_CACHE
        
        # Generate a stable hash for the text
        text_hash = get_text_hash(resume_text)
        
        # Check global cache
        if text_hash in _GLOBAL_CACHE:
            logger.info("Using cached resume processing result")
            return _GLOBAL_CACHE[text_hash]
        
        # Generate a unique ID for this resume
        resume_id = str(uuid.uuid4())
        
        # Track processing time
        start_time = time.time()
        
        # Start embedding generation in parallel if OpenSearch is enabled
        embedding_thread = None
        embedding_result = None
        if ENABLE_OPENSEARCH and self.opensearch_handler:
            try:
                import threading
                from queue import Queue
                
                # Use a reduced version of the text for embeddings to speed up processing
                compressed_text = self._compress_text(resume_text)
                
                # Create a queue to store the embedding result
                embedding_queue = Queue()
                
                def generate_embedding():
                    try:
                        # Generate embedding in background
                        from src.utils.bedrock_embeddings import BedrockEmbeddings
                        embeddings_client = BedrockEmbeddings()
                        embedding = embeddings_client.get_embedding(compressed_text, dimension=512)
                        embedding_queue.put(embedding)
                        logger.info("Generated embedding in background thread")
                    except Exception as e:
                        logger.error(f"Error generating embedding in background: {str(e)}")
                        embedding_queue.put(None)
                
                # Start background thread for embedding generation
                embedding_thread = threading.Thread(target=generate_embedding)
                embedding_thread.daemon = True
                embedding_thread.start()
                logger.info("Started background thread for embedding generation")
            except Exception as e:
                logger.error(f"Error setting up embedding thread: {str(e)}")
        
        # Extract structured data using LLM
        structured_data = self._extract_structured_data(resume_text)
        
        # Add resume_id to the data
        structured_data['resume_id'] = resume_id
        
        # Store in OpenSearch if enabled
        if ENABLE_OPENSEARCH and self.opensearch_handler:
            # Use a reduced version of the text for embeddings to speed up processing
            compressed_text = self._compress_text(resume_text)
            
            try:
                import threading
                
                def store_in_opensearch():
                    try:
                        logger.info(f"Storing resume data in OpenSearch with ID: {resume_id}")
                        
                        # If we have a pre-generated embedding, use it
                        embedding = None
                        if embedding_thread and embedding_thread.is_alive():
                            logger.info("Waiting for background embedding generation to complete")
                            embedding_thread.join(timeout=5)  # Wait up to 5 seconds
                        
                        if 'embedding_queue' in locals() and not locals()['embedding_queue'].empty():
                            embedding = locals()['embedding_queue'].get()
                            logger.info("Using pre-generated embedding for OpenSearch")
                            
                            # Store in OpenSearch with the pre-generated embedding
                            if embedding:
                                # Pad to 1024 if needed for schema compatibility
                                if len(embedding) < 1024:
                                    embedding = embedding + [0.0] * (1024 - len(embedding))
                                    
                                # Add embedding directly to resume_data to avoid regeneration
                                structured_data['_embedding_vector'] = embedding
                        
                        # Store in OpenSearch
                        success = self.opensearch_handler.store_resume(
                            resume_data=structured_data,
                            resume_id=resume_id,
                            resume_text=compressed_text
                        )
                        
                        if success:
                            logger.info(f"Successfully stored resume in OpenSearch with ID: {resume_id}")
                        else:
                            logger.error(f"Failed to store resume in OpenSearch with ID: {resume_id}")
                    except Exception as e:
                        logger.error(f"Error storing resume in OpenSearch: {str(e)}")
                
                # Start background thread for OpenSearch storage
                thread = threading.Thread(target=store_in_opensearch)
                thread.daemon = True
                thread.start()
                
                # Mark as successful for now since it's happening in background
                structured_data['opensearch_success'] = True
                
            except Exception as e:
                logger.error(f"Error setting up OpenSearch storage thread: {str(e)}")
                structured_data['opensearch_success'] = False
        
        # Log processing time
        processing_time = time.time() - start_time
        logger.info(f"Resume processed in {processing_time:.2f} seconds")
        
        # Add processing time to metrics
        structured_data['processing_time'] = processing_time
        
        # Cache the result
        result = (structured_data, resume_id)
        _GLOBAL_CACHE[text_hash] = result
        
        # Trim cache if needed
        if len(_GLOBAL_CACHE) > _CACHE_MAX_SIZE:
            # Remove a random item (simple approach)
            try:
                _GLOBAL_CACHE.pop(next(iter(_GLOBAL_CACHE)))
            except:
                pass
        
        return result
    
    def _compress_text(self, text: str) -> str:
        """Compress text to reduce processing time for embeddings"""
        # Keep only first 5000 chars (enough for semantic understanding)
        if len(text) > 5000:
            return text[:5000]
        return text
    
    def _extract_structured_data(self, resume_text: str) -> Dict[str, Any]:
        """
        Extract structured data from resume text using LLM
        
        Args:
            resume_text: Resume text content
            
        Returns:
            Structured resume data as dictionary
        """
        # Create prompt for the LLM
        prompt = self._create_extraction_prompt(resume_text)
        
        # Generate structured data using LLM
        try:
            llm_response = self.bedrock_client.generate_text(
                prompt=prompt,
                max_tokens=2000,  # Reduced from 4000 to speed up processing
                temperature=0.0,  # Use lower temperature for more deterministic output
                top_p=0.9,
                use_cache=True  # Enable response caching
            )
            
            # Parse the JSON output
            return self._parse_llm_response(llm_response)
            
        except Exception as e:
            logger.error(f"Error extracting structured data: {str(e)}")
            return {}
    
    def _create_extraction_prompt(self, resume_text: str) -> str:
        """
        Create a prompt for extracting structured data from resume text
        
        Args:
            resume_text: Resume text content
            
        Returns:
            Prompt string for LLM
        """
        # Create a prompt for the LLM
        prompt = """
You are an expert resume parser. Extract information strictly using these rules:

1. Dates: Format as "MM/YYYY-MM/YYYY" (e.g., "01/2022-12/2023")
2. Skills: Normalize to official names (e.g., "LoadRunner" not "Load Runner")
3. Industries: Use official NAICS codes + names (e.g., "5112 - Software Publishers")
4. Certifications: Include issuing body (e.g., "AWS Certified Solutions Architect - Associate (Amazon)")
5. Projects: Extract technologies separately from descriptions
6. Education: Convert years to integer (e.g., 2016 not "2012-2016")

Return JSON with these fields:
{
  "full_name": "",
  "email": "",
  "phone_number": "",
  "linkedin": "",
  "address": "",
  "summary": "(Generate 100+ words if missing)",
  "total_experience": "(Total years of experience, if not provided, calculate from dates)",
  "skills": ["(Standardized names)"],
  "positions": ["(Normalized titles)"],
  "companies": [
    {
      "name": "",
      "duration": "MM/YYYY-MM/YYYY", 
      "description": "",
      "role": "(From position)",
      "technologies": ["(Specific tools used)"]
    }
  ],
  "education": [
    {
      "degree": "",
      "institution": "",
      "year": "(Integer)"
    }
  ],
  "certifications": ["(With issuer)"],
  "achievements": [
    {
      "type": "(Performance/Innovation/Leadership)",
      "description": "",
      "metrics": "(Quantifiable impact)"
    }
  ],
  "industries": ["(NAICS code + name)"],
  "projects": [
  {
    "name": "Name of the project",
    "description": "Description of the project  ",
    "technologies": ["(Specific tools used)"],
    "duration_months": "(Duration in months)",
    "role": "(From position)",
    "metrics": "(Quantifiable impact)"
  }
]
}
RESUME TEXT:
{resume_text}

JSON OUTPUT:
"""

        # Replace placeholder with actual resume text
        prompt = prompt.replace("{resume_text}", resume_text)
        
        return prompt
    
    def _parse_llm_response(self, llm_response: str) -> Dict[str, Any]:
        """
        Parse the LLM response into a structured dictionary
        
        Args:
            llm_response: Raw LLM response text
            
        Returns:
            Structured dictionary
        """
        try:
            # Try to extract JSON from the response
            json_start = llm_response.find('{')
            json_end = llm_response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = llm_response[json_start:json_end]
                return json.loads(json_str)
            else:
                logger.error("Could not find JSON in LLM response")
                logger.debug(f"Raw response: {llm_response[:500]}...")
                return {}
                
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from LLM response: {str(e)}")
            logger.debug(f"Raw response: {llm_response[:500]}...")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error parsing LLM response: {str(e)}")
            return {}
    
    def extract_resume_data(self, resume_text: str) -> Dict[str, Any]:
        """
        Extract structured resume data with the specified schema
        
        Args:
            resume_text: Raw text extracted from the resume document
            
        Returns:
            Dictionary with structured resume data
        """
        # Calculate the approximate maximum text length based on model limits
        # Most LLMs have token limits (4k-16k tokens). A token is ~4-6 characters on average.
        # The prompt template also consumes tokens, so we need to leave room for it
        
        # Get the empty prompt template to calculate its approximate token length
        empty_prompt = self._create_extraction_prompt("")
        
        # Estimate token count using configured values
        CHAR_PER_TOKEN = BEDROCK_CHAR_PER_TOKEN  # From config
        TARGET_MAX_TOKENS = BEDROCK_MAX_INPUT_TOKENS  # From config
        
        # Calculate max resume text length
        estimated_prompt_tokens = len(empty_prompt) / CHAR_PER_TOKEN
        max_resume_tokens = TARGET_MAX_TOKENS - estimated_prompt_tokens
        max_resume_chars = int(max_resume_tokens * CHAR_PER_TOKEN)
        
        # Truncate resume text if needed
        original_length = len(resume_text)
        if original_length > max_resume_chars:
            logger.warning(f"Resume text exceeds estimated token limit. Truncating from {original_length} to {max_resume_chars} characters")
            resume_text = resume_text[:max_resume_chars] + "\n\n[Content truncated due to length]"
        
        # Get the prompt from the prompt module
        prompt = self._create_extraction_prompt(resume_text)
        
        try:
            # Invoke Bedrock model
            response = self.bedrock_client.invoke_model(prompt, max_tokens=4000, temperature=0.2)
            
            # Extract and parse JSON from response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {str(e)}")
                    logger.debug(f"Response snippet: {response[:200]}...")
                    
                    # Try to fix common JSON issues
                    logger.info("Attempting to fix JSON formatting issues")
                    fixed_json = self._fix_json(json_str)
                    if fixed_json:
                        logger.info("JSON fixed successfully")
                        return fixed_json
                    
                    # Create a minimal fallback response
                    return self._create_fallback_response(resume_text)
            else:
                logger.error("Could not find JSON object in LLM response")
                logger.debug(f"Response snippet: {response[:200]}...")
                
                # Try to find JSON-like content
                json_obj = self._extract_json_from_text(response)
                if json_obj:
                    return json_obj
                
                # Create a minimal fallback response
                return self._create_fallback_response(resume_text)
                
        except Exception as e:
            logger.error(f"Error extracting resume data: {str(e)}")
            return self._create_fallback_response(resume_text)
    
    def _fix_json(self, json_str: str) -> Dict[str, Any]:
        """
        Try to fix common JSON formatting issues
        
        Args:
            json_str: Potentially invalid JSON string
            
        Returns:
            Parsed JSON object or empty dict if parsing fails
        """
        try:
            # Try to fix common issues
            # 1. Replace single quotes with double quotes (but not inside strings)
            # 2. Fix trailing commas
            # 3. Add missing quotes to property names
            
            # First try direct parsing
            try:
                return json.loads(json_str)
            except:
                pass
            
            # Replace trailing commas
            json_str = re.sub(r',\s*}', '}', json_str)
            json_str = re.sub(r',\s*]', ']', json_str)
            
            # Try parsing again
            try:
                return json.loads(json_str)
            except:
                pass
            
            # Try with demjson if available (more lenient parser)
            try:
                import demjson3
                return demjson3.decode(json_str)
            except ImportError:
                pass
            except Exception:
                pass
                
            return {}
        except Exception as e:
            logger.error(f"Error fixing JSON: {str(e)}")
            return {}
    
    def _extract_json_from_text(self, text: str) -> Dict[str, Any]:
        """
        Try to extract JSON objects from text that might have markdown or other content
        
        Args:
            text: Text that might contain JSON
            
        Returns:
            Extracted JSON or empty dict
        """
        # Look for JSON in code blocks
        json_match = re.search(r'```(?:json)?\s*({[\s\S]*?})\s*```', text)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except:
                pass
        
        # Try to find any JSON-like object
        matches = re.findall(r'{[\s\S]*?}', text)
        for match in matches:
            try:
                obj = json.loads(match)
                if isinstance(obj, dict) and len(obj) > 3:  # Must have at least 3 keys to be considered valid
                    return obj
            except:
                continue
                
        return {}
    
    def _create_fallback_response(self, resume_text: str) -> Dict[str, Any]:
        """
        Create a minimal fallback response with basic info
        
        Args:
            resume_text: Original resume text
            
        Returns:
            Basic structured response
        """
        logger.warning("Creating fallback response with minimal data")
        
        # Extract basic info using regex patterns
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        phone_pattern = r'\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b'
        
        emails = re.findall(email_pattern, resume_text)
        phones = re.findall(phone_pattern, resume_text)
        
        return {
            "full_name": "Unknown",
            "email": ", ".join(emails).lower() if emails else "",
            "phone_number": ", ".join(phones) if phones else "",
            "address": "Not provided",
            "linkedin": "",
            "summary": "Failed to extract summary from resume.",
            "total_experience": 0,
            "positions": [],
            "companies": [],
            "education": [],
            "skills": [],
            "industries": [],
            "projects": [],
            "achievements": [],
            "certifications": []
        } 