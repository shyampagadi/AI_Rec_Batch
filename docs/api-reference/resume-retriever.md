# Resume Retriever API Reference

This document provides reference documentation for the Resume Retriever component of the Resume Parser & Matching System.

## Overview

The Resume Retriever is responsible for matching job descriptions with relevant resumes using vector embeddings, text search, and a multi-factor reranking algorithm. It serves as the main interface for performing job description matching operations.

## Class Reference

### `ResumeRetriever`

```python
class ResumeRetriever:
    """Class for retrieving and ranking resumes based on job descriptions"""
    
    def __init__(self, opensearch_handler=None, embedding_client=None):
        """
        Initialize the Resume Retriever.
        
        Args:
            opensearch_handler (OpenSearchHandler, optional): OpenSearch client.
                If None, creates a new instance.
            embedding_client (BedrockEmbeddings, optional): Embeddings client.
                If None, creates a new instance.
        """
```

## Core Methods

### Search and Retrieval

#### `search_resumes`

```python
def search_resumes(
    self, 
    jd_file=None,
    jd_text=None,
    method="vector",
    size=20, 
    required_experience=3.0,
    preferred_experience=None,
    enable_reranking=True,
    reranking_weights=None,
    min_score=0,
    output_file=None
):
    """
    Search for resumes matching a job description.
    
    Args:
        jd_file (str, optional): Path to job description file.
        jd_text (str, optional): Job description text (alternative to jd_file).
        method (str): Search method ('vector', 'text', or 'hybrid'). Defaults to 'vector'.
        size (int): Maximum number of results to return. Defaults to 20.
        required_experience (float): Required years of experience. Defaults to 3.0.
        preferred_experience (float, optional): Preferred years of experience.
        enable_reranking (bool): Whether to apply reranking algorithm. Defaults to True.
        reranking_weights (dict, optional): Custom weights for reranking.
            Format: {'vector': 0.5, 'skill': 0.25, 'experience': 0.15, 'recency': 0.1}
        min_score (float): Minimum score to include in results. Defaults to 0.
        output_file (str, optional): Path to save results. If None, auto-generates name.
        
    Returns:
        dict: Results containing job description analysis and matched candidates
        
    Raises:
        ValueError: If neither jd_file nor jd_text is provided
        FileNotFoundError: If job description file doesn't exist
    """
```

#### `_vector_search`

```python
def _vector_search(self, query_text, size=10, enable_reranking=True):
    """
    Search for resumes using vector similarity with optional reranking.
    
    Args:
        query_text (str): Job description text
        size (int): Maximum number of results to return
        enable_reranking (bool): Whether to apply reranking algorithm
        
    Returns:
        list: List of matching documents with scores
    """
```

#### `_text_search`

```python
def _text_search(self, query_text, size=10, enable_reranking=True):
    """
    Search for resumes using text matching with optional reranking.
    
    Args:
        query_text (str): Job description text
        size (int): Maximum number of results to return
        enable_reranking (bool): Whether to apply reranking algorithm
        
    Returns:
        list: List of matching documents with scores
    """
```

#### `_hybrid_search`

```python
def _hybrid_search(
    self, 
    query_text, 
    size=10, 
    vector_weight=0.7, 
    text_weight=0.3, 
    enable_reranking=True
):
    """
    Search for resumes using hybrid approach (vector + text) with optional reranking.
    
    Args:
        query_text (str): Job description text
        size (int): Maximum number of results to return
        vector_weight (float): Weight for vector search results (0.0-1.0)
        text_weight (float): Weight for text search results (0.0-1.0)
        enable_reranking (bool): Whether to apply reranking algorithm
        
    Returns:
        list: List of matching documents with scores
    """
```

### Reranking Algorithm

#### `_rerank_results`

```python
def _rerank_results(
    self, 
    candidates, 
    job_description, 
    required_experience=0, 
    preferred_experience=None,
    weights=None
):
    """
    Rerank search results using multiple factors.
    
    Args:
        candidates (list): Initial candidate results from search
        job_description (dict): Processed job description data
        required_experience (float): Minimum required experience
        preferred_experience (float, optional): Preferred experience level
        weights (dict, optional): Custom weights for scoring components
            Default: {'vector': 0.5, 'skill': 0.25, 'experience': 0.15, 'recency': 0.1}
        
    Returns:
        list: Reranked candidates with updated scores
    """
```

#### `_calculate_skill_score`

```python
def _calculate_skill_score(self, jd_skills, candidate_skills):
    """
    Calculate skill match score based on overlap between job requirements and candidate skills.
    
    Args:
        jd_skills (list): Skills required in job description
        candidate_skills (list): Skills listed in candidate's resume
        
    Returns:
        float: Skill match score (0-100)
    """
```

#### `_calculate_experience_score`

```python
def _calculate_experience_score(
    self, 
    required_experience, 
    candidate_experience, 
    preferred_experience=None
):
    """
    Calculate experience match score.
    
    Args:
        required_experience (float): Minimum years of experience required
        candidate_experience (float): Candidate's years of experience
        preferred_experience (float, optional): Preferred years of experience
        
    Returns:
        float: Experience match score (0-100)
    """
```

#### `_calculate_recency_score`

```python
def _calculate_recency_score(self, positions, job_skills):
    """
    Calculate score based on how recently relevant skills were used.
    
    Args:
        positions (list): Candidate's work positions
        job_skills (list): Skills required for the job
        
    Returns:
        float: Recency score (0-100)
    """
```

### Job Description Processing

#### `_process_job_description`

```python
def _process_job_description(self, jd_text):
    """
    Process job description to extract key information.
    
    Args:
        jd_text (str): Job description text
        
    Returns:
        dict: Structured job description data with skills, experience, etc.
    """
```

#### `_extract_skills_from_jd`

```python
def _extract_skills_from_jd(self, jd_text):
    """
    Extract required skills from job description text.
    
    Args:
        jd_text (str): Job description text
        
    Returns:
        list: List of required skills
    """
```

#### `_extract_experience_requirement`

```python
def _extract_experience_requirement(self, jd_text):
    """
    Extract years of experience required from job description.
    
    Args:
        jd_text (str): Job description text
        
    Returns:
        float: Required years of experience (0 if not specified)
    """
```

## Utility Methods

#### `_normalize_score`

```python
def _normalize_score(self, raw_score, min_score=0, max_score=1):
    """
    Normalize a raw score to the 0-100 range.
    
    Args:
        raw_score (float): Original score value
        min_score (float): Minimum possible score
        max_score (float): Maximum possible score
        
    Returns:
        float: Normalized score between 0-100
    """
```

#### `_save_results`

```python
def _save_results(self, results, output_file=None):
    """
    Save search results to a file.
    
    Args:
        results (dict): Search results
        output_file (str, optional): Output file path. If None, auto-generates name.
        
    Returns:
        str: Path to saved file
    """
```

## Caching

The ResumeRetriever implements caching to improve performance for repeated queries:

```python
# Module-level caches
_vector_search_cache = {}
_text_search_cache = {}
_hybrid_search_cache = {}
_jd_processing_cache = {}

def _get_cached_search(self, cache_dict, query_text, size):
    """Get cached search results if available"""
    cache_key = f"{query_text}::{size}"
    return cache_dict.get(cache_key)

def _cache_search_results(self, cache_dict, query_text, size, results):
    """Cache search results for future use"""
    cache_key = f"{query_text}::{size}"
    cache_dict[cache_key] = results
```

## Configuration Options

The ResumeRetriever can be configured via environment variables:

| Env Variable | Default | Description |
|--------------|---------|-------------|
| OPENSEARCH_HOST | None (required) | OpenSearch domain endpoint |
| AWS_REGION | None (required) | AWS region |
| OPENSEARCH_INDEX_NAME | "resumes" | OpenSearch index name |
| CACHE_ENABLED | "True" | Whether to use caching |
| CACHE_SIZE | "100" | Maximum number of cached results per type |
| DEFAULT_VECTOR_WEIGHT | "0.5" | Default weight for vector similarity |
| DEFAULT_SKILL_WEIGHT | "0.25" | Default weight for skill matching |
| DEFAULT_EXPERIENCE_WEIGHT | "0.15" | Default weight for experience matching |
| DEFAULT_RECENCY_WEIGHT | "0.1" | Default weight for recency matching |

## Usage Examples

### Basic Search

```python
from retrieve_jd_matches import ResumeRetriever

# Initialize retriever
retriever = ResumeRetriever()

# Search with job description file
results = retriever.search_resumes(
    jd_file="job_descriptions/software_engineer.txt",
    size=20
)

# Process results
for candidate in results["matches"]:
    print(f"Candidate: {candidate.get('name')}")
    print(f"Score: {candidate.get('score')}")
    print(f"Matching skills: {', '.join(candidate.get('matched_skills'))}")
    print(f"Years of experience: {candidate.get('years_of_experience')}")
    print("-" * 40)
```

### Direct Text Search

```python
# Search with job description text
results = retriever.search_resumes(
    jd_text="Software Engineer with Python and AWS experience needed for developing cloud applications...",
    method="vector",
    size=10,
    required_experience=5.0
)
```

### Custom Reranking Weights

```python
# Prioritize skill matching over semantic similarity
results = retriever.search_resumes(
    jd_file="job_descriptions/data_scientist.txt",
    reranking_weights={
        'vector': 0.3,
        'skill': 0.5,
        'experience': 0.15,
        'recency': 0.05
    }
)
```

### Hybrid Search

```python
# Use hybrid search with custom vector/text ratio
results = retriever.search_resumes(
    jd_file="job_descriptions/frontend_developer.txt",
    method="hybrid",
    size=15,
    hybrid_vector_weight=0.6,
    hybrid_text_weight=0.4
)
```

### Disable Reranking

```python
# Use raw vector search without reranking
results = retriever.search_resumes(
    jd_file="job_descriptions/product_manager.txt",
    method="vector",
    enable_reranking=False
)
```

## Result Structure

The search results have the following structure:

```python
{
    "job_description": {
        "content": "Full text of the job description...",
        "skills": ["Python", "AWS", "Docker", "Kubernetes"],
        "required_experience": 5.0,
        "embedding_id": "jd_20230501_123456"
    },
    "matches": [
        {
            "resume_id": "resume_abc123",
            "name": "John Doe",
            "score": 87.5,
            "vector_score": 85.2,
            "skill_score": 90.0,
            "experience_score": 95.0,
            "recency_score": 75.0,
            "years_of_experience": 6.5,
            "matched_skills": ["Python", "AWS", "Docker"],
            "missing_skills": ["Kubernetes"],
            "email": "john.doe@example.com",
            "phone": "+1-234-567-8900",
            "current_position": "Senior Software Engineer at XYZ Corp"
        },
        # More candidates...
    ],
    "metadata": {
        "search_method": "vector",
        "reranking_enabled": true,
        "reranking_weights": {
            "vector": 0.5,
            "skill": 0.25,
            "experience": 0.15,
            "recency": 0.1
        },
        "timestamp": "2023-05-01T14:32:45Z",
        "total_candidates": 1250,
        "search_time_ms": 245
    }
}
```

## Best Practices

1. **Job Description Preparation**
   - Structure job descriptions with clear skills sections
   - Be explicit about experience requirements
   - Include key responsibilities and qualifications

2. **Search Method Selection**
   - Use vector search for general semantic matching
   - Use text search when specific terminology is critical
   - Use hybrid search to balance both approaches

3. **Performance Optimization**
   - Utilize caching for repeated searches
   - Use appropriate result sizes (10-20 for most cases)
   - Consider disabling reranking for very large candidate pools

4. **Reranking Customization**
   - Adjust weights based on job requirements
   - Increase skill weight for technical positions
   - Increase vector weight for more holistic matching 