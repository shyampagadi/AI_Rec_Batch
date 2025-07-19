# Bedrock Embeddings API Reference

This document provides reference documentation for the AWS Bedrock Embeddings component of the Resume Parser & Matching System.

## Overview

The Bedrock Embeddings module provides an interface to AWS Bedrock for generating vector embeddings from text. These embeddings are used for semantic search and matching of resumes and job descriptions.

## Class Reference

### `BedrockEmbeddings`

```python
class BedrockEmbeddings:
    """Class to handle embeddings generation using Amazon Bedrock"""
    
    def __init__(self, model_id=None, region=None, profile=None):
        """
        Initialize the Bedrock client.
        
        Args:
            model_id (str, optional): Bedrock model ID to use. Defaults to env var BEDROCK_EMBEDDINGS_MODEL
                or "amazon.titan-embed-text-v2:0".
            region (str, optional): AWS region. Defaults to env var AWS_REGION.
            profile (str, optional): AWS profile. Defaults to env var AWS_PROFILE or None.
        """
```

## Core Methods

### Embedding Generation

#### `get_embedding`

```python
def get_embedding(self, text: str, dimension: int = 1024) -> List[float]:
    """
    Get embedding vector for a single text.
    
    Args:
        text (str): Text to generate embedding for
        dimension (int, optional): Embedding dimension. Defaults to 1024.
            Note: Some models have fixed dimensions and will ignore this parameter.
        
    Returns:
        List[float]: Vector embedding
        
    Raises:
        BedrockError: If Bedrock embedding generation fails
        ValueError: If input validation fails
    """
```

#### `get_embeddings_batch`

```python
def get_embeddings_batch(self, texts: List[str], dimension: int = 1024) -> List[List[float]]:
    """
    Get embeddings for a batch of texts.
    
    Args:
        texts (List[str]): List of texts to generate embeddings for
        dimension (int, optional): Embedding dimension. Defaults to 1024.
        
    Returns:
        List[List[float]]: List of vector embeddings
        
    Raises:
        BedrockError: If Bedrock embedding generation fails
        ValueError: If input validation fails
    """
```

#### `normalize_embedding`

```python
def normalize_embedding(self, embedding: List[float]) -> List[float]:
    """
    Normalize embedding vector to unit length (for cosine similarity).
    
    Args:
        embedding (List[float]): Original embedding vector
        
    Returns:
        List[float]: Normalized vector
    """
```

## Supported Models

The `BedrockEmbeddings` class supports several AWS Bedrock embedding models, each with specific features and request formats:

### Amazon Titan Embed

```python
def _call_titan_embed(self, text: str, dimension: int = 1024) -> List[float]:
    """
    Call Amazon Titan Embed model to generate embeddings.
    
    Args:
        text (str): Text to embed
        dimension (int): Embedding dimension (supported: 1024, 1536, 2048, 3072)
        
    Returns:
        List[float]: Embedding vector
    """
```

### Cohere Embed

```python
def _call_cohere_embed(self, text: str, dimension: int = 1024) -> List[float]:
    """
    Call Cohere Embed model to generate embeddings.
    
    Args:
        text (str): Text to embed
        dimension (int): Embedding dimension (supported: varies by model)
        
    Returns:
        List[float]: Embedding vector
    """
```

### Meta Llama Embed

```python
def _call_meta_embed(self, text: str, dimension: int = 1024) -> List[float]:
    """
    Call Meta Llama embedding model to generate embeddings.
    
    Args:
        text (str): Text to embed
        dimension (int): Embedding dimension (supported: varies by model)
        
    Returns:
        List[float]: Embedding vector
    """
```

## Utility Methods

#### `validate_text`

```python
def validate_text(self, text: str) -> str:
    """
    Validate and preprocess text for embedding.
    
    Args:
        text (str): Text to validate
        
    Returns:
        str: Preprocessed text
        
    Raises:
        ValueError: If text is empty or not a string
    """
```

#### `get_model_info`

```python
def get_model_info(self) -> dict:
    """
    Get information about the current embedding model.
    
    Returns:
        dict: Model information including ID, provider, and capabilities
    """
```

## Caching

The module includes an embedding cache to avoid regenerating embeddings for the same text:

```python
# Module-level cache
_embedding_cache = {}

def _get_cached_embedding(text: str, dimension: int, model_id: str) -> Optional[List[float]]:
    """Get embedding from cache if available"""
    cache_key = f"{model_id}:{dimension}:{hashlib.md5(text.encode()).hexdigest()}"
    return _embedding_cache.get(cache_key)

def _cache_embedding(text: str, embedding: List[float], dimension: int, model_id: str) -> None:
    """Cache an embedding for future use"""
    cache_key = f"{model_id}:{dimension}:{hashlib.md5(text.encode()).hexdigest()}"
    _embedding_cache[cache_key] = embedding
```

## Error Handling

The module provides custom error handling with detailed error messages:

```python
try:
    response = self.bedrock_runtime.invoke_model(
        modelId=self.embedding_model_id,
        body=json.dumps(request_body)
    )
    # Process response...
except Exception as e:
    # Transform error for better debugging
    error_message = f"Bedrock embedding error: {str(e)}"
    if "AccessDeniedException" in str(e):
        error_message = f"Access denied to Bedrock model {self.embedding_model_id}. Check IAM permissions."
    elif "ValidationException" in str(e):
        error_message = f"Validation error with Bedrock request: {str(e)}"
    elif "ModelNotReadyException" in str(e):
        error_message = f"Model {self.embedding_model_id} is not ready. Try again later."
    elif "ThrottlingException" in str(e):
        error_message = f"Bedrock API rate limit exceeded. Try again later."
        
    logging.error(error_message)
    raise BedrockError(error_message)
```

## Configuration Options

The BedrockEmbeddings module can be configured via environment variables or constructor parameters:

| Parameter | Env Variable | Default | Description |
|-----------|--------------|---------|-------------|
| model_id | BEDROCK_EMBEDDINGS_MODEL | "amazon.titan-embed-text-v2:0" | Bedrock model ID |
| region | AWS_REGION | None (required) | AWS region |
| profile | AWS_PROFILE | None | AWS profile name |

Additional configuration options can be set via environment variables:

| Env Variable | Default | Description |
|--------------|---------|-------------|
| EMBEDDING_CACHE_SIZE | 1000 | Maximum number of embeddings to cache |
| EMBEDDING_TIMEOUT | 30 | Timeout in seconds for embedding API calls |
| EMBEDDING_RETRIES | 3 | Number of retries for failed API calls |

## Usage Examples

### Basic Embedding Generation

```python
from src.utils.bedrock_embeddings import BedrockEmbeddings

# Initialize the client
embeddings_client = BedrockEmbeddings()

# Generate embedding
text = "Software engineer with 5 years of experience in Python"
embedding = embeddings_client.get_embedding(text)

# Use embedding for vector search or storage
print(f"Generated embedding with {len(embedding)} dimensions")
```

### Batch Processing

```python
# Process multiple texts in batch
texts = [
    "Software engineer with Python experience",
    "Data scientist with machine learning expertise",
    "Project manager with agile methodology background"
]

embeddings = embeddings_client.get_embeddings_batch(texts)

for i, emb in enumerate(embeddings):
    print(f"Text {i+1} embedding: {len(emb)} dimensions")
```

### Custom Dimension

```python
# Generate lower-dimension embedding for efficiency
text = "Front-end developer with React experience"
embedding = embeddings_client.get_embedding(text, dimension=768)
```

### With Specific Model

```python
# Use specific embedding model
embeddings_client = BedrockEmbeddings(model_id="cohere.embed-english-v3")

text = "DevOps engineer with Kubernetes experience"
embedding = embeddings_client.get_embedding(text)
```

## Model Selection Guide

| Model ID | Dimensions | Strengths | Best For |
|----------|------------|-----------|----------|
| amazon.titan-embed-text-v2:0 | 1536 | Balance of quality/cost | General purpose |
| amazon.titan-embed-text-v1:0 | 1024 | Efficient, fast | High-volume, less sensitive |
| cohere.embed-english-v3 | 1024 | High quality multilingual | Complex text, multiple languages |
| cohere.embed-multilingual-v3 | 1024 | Best multilingual | International resumes |
| meta.llama-3-8b-embed:0 | 4096 | Very high quality | Research, high precision needs |

## Best Practices

1. **Text Preparation**
   - Clean and normalize text before generating embeddings
   - Remove irrelevant content (e.g., headers, footers)
   - Structure important information first

2. **Performance Optimization**
   - Use batch processing for multiple texts
   - Enable caching for repeated texts
   - Choose appropriate dimensions (lower = faster)

3. **Error Handling**
   - Implement retries with exponential backoff
   - Handle model-specific errors appropriately
   - Monitor rate limits and costs

4. **Dimension Selection**
   - Higher dimensions capture more semantic nuance
   - Lower dimensions reduce storage and search costs
   - Match dimensions to downstream system capabilities

5. **Model Selection**
   - Test multiple models for your specific domain
   - Consider cost vs. quality trade-offs
   - Monitor performance metrics across models 