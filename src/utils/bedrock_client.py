import json
import logging
import os
import time
import functools
from typing import Dict, Any, Optional, List
import boto3
from botocore.config import Config

from config.config import (
    AWS_REGION,
    BEDROCK_MODEL_ID,
    get_aws_credentials
)

logger = logging.getLogger(__name__)

# Global client cache to avoid creating multiple clients
_bedrock_clients = {}

# Response cache for LLM calls
class ResponseCache:
    def __init__(self, max_size=100):
        self.cache = {}
        self.max_size = max_size
        self.access_order = []
    
    def get(self, key):
        if key in self.cache:
            # Update access order
            self.access_order.remove(key)
            self.access_order.append(key)
            return self.cache[key]
        return None
    
    def put(self, key, value):
        if len(self.cache) >= self.max_size:
            # Remove least recently used item
            lru_key = self.access_order.pop(0)
            del self.cache[lru_key]
        
        self.cache[key] = value
        self.access_order.append(key)

# Global response cache
_response_cache = ResponseCache(max_size=200)

def get_bedrock_client(region=None, model_id=None):
    """
    Get or create a Bedrock client
    
    Args:
        region: AWS region
        model_id: Bedrock model ID
        
    Returns:
        boto3 Bedrock client
    """
    global _bedrock_clients
    
    region = region or AWS_REGION
    model_id = model_id or BEDROCK_MODEL_ID
    
    # Use model ID and region as cache key
    cache_key = f"{region}:{model_id}"
    
    if cache_key not in _bedrock_clients:
        # Configure retry settings
        config = Config(
            retries={
                'max_attempts': 3,
                'mode': 'adaptive'
            },
            connect_timeout=10,
            read_timeout=60
        )
        
        # Create client with credentials
        credentials = get_aws_credentials()
        _bedrock_clients[cache_key] = boto3.client(
            service_name='bedrock-runtime',
            region_name=region,
            config=config,
            **credentials
        )
        
        logger.info(f"Created new Bedrock client for region {region} and model {model_id}")
    
    return _bedrock_clients[cache_key]

class BedrockClient:
    """Client for AWS Bedrock API"""
    
    def __init__(self, model_id: Optional[str] = None, region: Optional[str] = None):
        """
        Initialize Bedrock client
        
        Args:
            model_id: Bedrock model ID (default: from config)
            region: AWS region (default: from config)
        """
        self.model_id = model_id or BEDROCK_MODEL_ID
        self.region = region or AWS_REGION
        
        # Get cached client
        self.client = get_bedrock_client(self.region, self.model_id)
        
        # Track performance metrics
        self.total_tokens = 0
        self.total_time = 0
        self.total_calls = 0
        
        logger.info(f"Initialized Bedrock client with model: {self.model_id}")
    
    def _get_cache_key(self, prompt, model_kwargs):
        """Generate a cache key from prompt and model parameters"""
        # Create a string representation of model_kwargs
        kwargs_str = json.dumps(model_kwargs, sort_keys=True)
        return f"{hash(prompt)}:{hash(kwargs_str)}"
    
    def generate_text(
        self,
        prompt: str,
        max_tokens: int = 2000,
        temperature: float = 0.0,
        top_p: float = 0.9,
        stop_sequences: Optional[List[str]] = None,
        use_cache: bool = True
    ) -> str:
        """
        Generate text using AWS Bedrock
        
        Args:
            prompt: Input prompt
            max_tokens: Maximum tokens to generate
            temperature: Temperature for sampling
            top_p: Top-p sampling parameter
            stop_sequences: Sequences that stop generation
            use_cache: Whether to use response caching
            
        Returns:
            Generated text
        """
        # Adaptive token estimation based on model
        CHAR_PER_TOKEN = 4.0
        MAX_INPUT_TOKENS = 8192  # Most conservative - Llama3's limit
        
        # Model-specific limits
        if 'claude' in self.model_id.lower():
            # Claude models can handle more context
            MAX_INPUT_TOKENS = 100000  # Claude 3 Opus
            CHAR_PER_TOKEN = 3.5
        elif 'meta.llama3' in self.model_id.lower():
            # Llama 3 models have 8K context
            MAX_INPUT_TOKENS = 8192
            CHAR_PER_TOKEN = 4.0
        elif 'amazon.titan' in self.model_id.lower():
            # Titan models typically have 32K context 
            MAX_INPUT_TOKENS = 32000
            CHAR_PER_TOKEN = 4.0
        
        # Account for output tokens
        usable_input_tokens = MAX_INPUT_TOKENS - max_tokens - 50  # Safety margin
        
        # Estimate prompt length in tokens
        estimated_tokens = len(prompt) / CHAR_PER_TOKEN
        
        # Smart truncation if needed
        if estimated_tokens > usable_input_tokens:
            # Calculate chars to keep
            max_chars = int(usable_input_tokens * CHAR_PER_TOKEN)
            original_len = len(prompt)
            
            logger.warning(f"Prompt too long ({estimated_tokens:.0f} tokens, limit {usable_input_tokens:.0f}). Truncating from {original_len} to {max_chars} chars")
            
            # Try to do smart truncation - keep beginning and ending
            if max_chars > 1000:
                # Keep beginning and end of the prompt
                beginning = prompt[:int(max_chars * 0.8)]  # Keep 80% from beginning
                ending = prompt[-(int(max_chars * 0.2)):]  # Keep 20% from end
                prompt = beginning + "\n...[content truncated for length]...\n" + ending
            else:
                # Simple truncation if very tight on tokens
                prompt = prompt[:max_chars]
                
            logger.info(f"Truncated prompt from {original_len} to {len(prompt)} chars")
        
        model_kwargs = {
            'max_tokens': max_tokens,
            'temperature': temperature,
            'top_p': top_p,
            'stop_sequences': stop_sequences or []
        }
        
        # Check cache if enabled
        if use_cache:
            cache_key = self._get_cache_key(prompt, model_kwargs)
            
            if cache_key in _response_cache.cache:
                logger.info("Using cached response for prompt")
                return _response_cache.cache[cache_key]
        
        # Track metrics
        start_time = time.time()
        self.total_calls += 1
        
        try:
            # Prepare request body based on model provider
            if 'anthropic' in self.model_id.lower():
                # Anthropic Claude models
                body = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "top_p": top_p,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                }
                
                if stop_sequences:
                    body["stop_sequences"] = stop_sequences
                
            elif 'meta' in self.model_id.lower():
                # Meta Llama models - use optimized settings
                body = {
                    "prompt": prompt,
                    "max_gen_len": max_tokens,
                    "temperature": temperature,
                    "top_p": top_p
                }
                
            elif 'amazon' in self.model_id.lower():
                # Amazon Titan models
                body = {
                    "inputText": prompt,
                    "textGenerationConfig": {
                        "maxTokenCount": max_tokens,
                        "temperature": temperature,
                        "topP": top_p,
                        "stopSequences": stop_sequences or []
                    }
                }
                
            else:
                # Default format (works with most models)
                body = {
                    "prompt": prompt,
                    "max_tokens_to_sample": max_tokens,
                    "temperature": temperature,
                    "top_p": top_p,
                    "stop_sequences": stop_sequences or []
                }
            
            # Make API call with single attempt (no retries for speed)
            response = self.client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body)
            )
            
            # Parse response based on model provider
            response_body = json.loads(response.get('body').read())
            
            if 'anthropic' in self.model_id.lower():
                # Anthropic Claude models
                result = response_body.get('content', [{}])[0].get('text', '')
                
            elif 'meta' in self.model_id.lower():
                # Meta Llama models
                result = response_body.get('generation', '')
                
            elif 'amazon' in self.model_id.lower():
                # Amazon Titan models
                result = response_body.get('results', [{}])[0].get('outputText', '')
                
            else:
                # Default format (works with most models)
                result = response_body.get('completion', '')
            
            # Update metrics
            elapsed_time = time.time() - start_time
            self.total_time += elapsed_time
            
            # Log performance metrics
            logger.info(f"Generated text in {elapsed_time:.2f}s")
            
            # Cache the result if enabled
            if use_cache:
                _response_cache.put(cache_key, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error generating text: {str(e)}")
            raise
    
    def extract_resume_data(self, resume_text: str) -> Dict[str, Any]:
        """Extract structured data from resume text using Bedrock LLM"""
        # Create a prompt instructing the LLM to extract information from the resume
        prompt = f"""
        You are a professional resume parser. Extract the following information from the resume text below.
        Format your response as a valid JSON object with these fields:
        
        - full_name: Candidate's full name 
        - email: All valid emails as a SINGLE STRING with values lowercase and comma separated
        - phone_number: All valid phone numbers as a SINGLE STRING with values comma separated
        - address: Extract ONLY valid address information, especially Indian address formats with PIN codes
        - linkedin: Valid LinkedIn profile URL
        - summary: Brief professional summary
        - total_experience: Candidate's total years of professional experience as a decimal number
        - positions: Array of job titles
        - companies: Array of company objects with name, duration, and projects
        - education: Array of education objects with degree, institution, and year
        - skills: Array of skills
        - industries: Array of industries the candidate has worked in
        - projects: Array of project objects with name, description, company, and technologies
        - achievements: Array of achievement objects with company, description, and category
        - certifications: Array of certification names
        
        Only include valid JSON in your response.
        
        Resume text:
        {resume_text}
        """
        
        try:
            # Invoke Bedrock model
            response = self.generate_text(prompt, max_tokens=4000, temperature=0.2)
            
            logger.debug(f"LLM Response: {response[:500]}...")
            
            # Try different approaches to extract JSON
            # First, look for JSON object pattern in the response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {str(e)}")
                    logger.debug(f"Failed JSON string: {json_str[:500]}...")
                    
                    # Try to clean up the JSON string
                    logger.info("Attempting to clean up and fix JSON string")
                    clean_json_str = self._clean_json_string(json_str)
                    
                    try:
                        return json.loads(clean_json_str)
                    except json.JSONDecodeError as e2:
                        logger.error(f"Clean JSON also failed: {str(e2)}")
                        return {}
            else:
                logger.error("Could not find JSON object in LLM response")
                return {}
                
        except Exception as e:
            logger.error(f"Error extracting resume data: {str(e)}")
            return {}
    
    def _clean_json_string(self, json_str: str) -> str:
        """Attempt to clean up and fix common JSON formatting issues"""
        # Replace common issues with proper JSON formatting
        replacements = [
            # Fix trailing commas in arrays
            (r',\s*]', ']'),
            # Fix trailing commas in objects
            (r',\s*}', '}'),
            # Fix missing quotes around property names
            (r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3'),
            # Fix single quotes to double quotes
            (r"'([^']*)'", r'"\1"'),
            # Fix unescaped newlines in strings
            (r'([":]\s*"[^"]*)\n([^"]*")', r'\1\\n\2'),
            # Fix unescaped tabs in strings
            (r'([":]\s*"[^"]*)\t([^"]*")', r'\1\\t\2'),
            # Fix unescaped backslashes 
            (r'([":]\s*"[^"]*)(\\)([^"\\][^"]*")', r'\1\\\\\3'),
        ]
        
        # Apply all replacements
        import re
        result = json_str
        for pattern, replacement in replacements:
            result = re.sub(pattern, replacement, result)
        
        return result 