# Vector Embedding Framework

This technical document provides a comprehensive explanation of the vector embedding framework used in the Resume Parser & Matching System.

## Overview

The vector embedding framework is the core technology that powers semantic search and matching capabilities in the system. It leverages AWS Bedrock's embedding models to convert text documents (resumes and job descriptions) into high-dimensional vector representations that capture the semantic meaning of the content.

## Key Components

### 1. Bedrock Embeddings Client

The `BedrockEmbeddings` class in `src/utils/bedrock_embeddings.py` provides the primary interface for generating embeddings:

```python
class BedrockEmbeddings:
    """Class to handle embeddings generation using Amazon Bedrock"""
    
    def __init__(self):
        """Initialize the Bedrock client"""
        self.bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            region_name=AWS_REGION
        )
        # Use embedding model from config instead of hardcoding
        self.embedding_model_id = BEDROCK_EMBEDDINGS_MODEL or "amazon.titan-embed-text-v2:0"
        
    def get_embedding(self, text: str, dimension: int = 1024) -> List[float]:
        """Get embedding vector for text"""
        # Implementation details...
```

The framework supports multiple embedding models, with special handling for:
- Amazon Titan Embed Text v1/v2
- Cohere Embed models
- Meta Llama embedding models

### 2. Text Standardization

Before generating embeddings, the system standardizes text using consistent templates:

```python
def create_standardized_text(data: Dict[str, Any]) -> str:
    """
    Create a standardized text representation for embedding generation
    """
    # Extract various fields from data dictionary
    # Format using COMMON_TEMPLATE
    return standardized_text

# Common template for standardized embedding generation
COMMON_TEMPLATE = """
SKILLS: {skills}
EXPERIENCE: {experience}
POSITIONS: {positions}
EDUCATION: {education}
TECHNOLOGIES: {technologies}
SUMMARY: {summary}
"""
```

This standardization ensures that:
1. Different resume formats are normalized to a common structure
2. Job descriptions follow the same structure as resumes
3. The most important information is prominently positioned
4. Irrelevant information is excluded

### 3. Resume-Specific Embedding Generation

For resumes, the system uses a specialized function to format content for optimal embedding relevance:

```python
def create_embedded_text(resume_data: Dict) -> str:
    """
    Converts parsed resume data into structured text for embeddings
    Returns: Formatted text string optimized for semantic search
    
    Note: Excludes PII data (name, email, phone, LinkedIn, address)
    """
    # Implementation that formats resume data into sections
```

This function creates a markdown-like structure that:
- Removes PII data to avoid bias
- Organizes content into semantic sections
- Highlights key skills, experience, and qualifications

## Embedding Dimensions and Parameters

The system uses the following configuration:
- **Default dimension**: 1024 (can be configured)
- **Vector normalization**: Cosine similarity space
- **Storage**: OpenSearch kNN vectors with HNSW algorithm
- **Model**: Configurable via `BEDROCK_EMBEDDINGS_MODEL` environment variable

## OpenSearch Schema Configuration

The vector embeddings are stored in OpenSearch using a specific schema:

```json
{
  "settings": {
    "index": {
      "knn": true,
      "knn.algo_param.ef_search": 1024,
      "analysis": {
        "normalizer": {
          "lowercase": {
            "type": "custom",
            "filter": ["lowercase"]
          }
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "resume_id": {
        "type": "keyword"
      },
      "resume_embedding": {
        "type": "knn_vector",
        "dimension": 1024,
        "method": {
          "name": "hnsw",
          "engine": "nmslib",
          "space_type": "cosinesimil",
          "parameters": {
            "ef_construction": 1024,
            "m": 48
          }
        }
      },
      // Other fields...
    }
  }
}
```

Key schema parameters:
- **ef_search**: 1024 (controls search accuracy vs. performance)
- **ef_construction**: 1024 (controls index quality)
- **m**: 48 (controls graph connectivity)
- **space_type**: cosinesimil (optimizes for cosine similarity)

## Embedding Generation Process

1. **Resume Parsing**: The `parse_resume.py` script processes a resume document
2. **Text Extraction**: The system extracts plain text content
3. **Data Structuring**: LLM extracts structured information (skills, experience, etc.)
4. **Text Standardization**: Structured data is converted to standardized text format
5. **Embedding Generation**: AWS Bedrock generates a 1024-dimensional vector
6. **Storage**: Vector is stored in OpenSearch alongside structured data

## Vector Search Implementation

The vector search is implemented in the `_vector_search` method of the `ResumeRetriever` class:

```python
def _vector_search(self, query_text: str, size: int = 10, enable_reranking: bool = True) -> List[Dict[str, Any]]:
    """
    Search for resumes using vector similarity with optional reranking
    """
    # Implementation details...
```

The search process:
1. **Query Text Processing**: Standardizes the job description text
2. **Query Embedding**: Generates an embedding vector for the query
3. **kNN Search**: Performs a k-nearest neighbors search in OpenSearch
4. **Result Retrieval**: Gets documents with metadata and scores
5. **Score Normalization**: Normalizes scores to a 0-100 scale
6. **Optional Reranking**: Applies additional ranking criteria (see Reranking Algorithm)

## Caching Mechanisms

To improve performance, the system implements several caching mechanisms:

1. **Embedding Cache**: Prevents regenerating embeddings for the same text
   ```python
   _embedding_cache = {}
   ```

2. **Search Result Cache**: Stores search results for repeated queries
   ```python
   _text_search_cache = {}
   _vector_search_cache = {}
   _hybrid_search_cache = {}
   ```

## Usage Examples

### Generating Embeddings

```python
from src.utils.bedrock_embeddings import BedrockEmbeddings

# Initialize the client
embeddings_client = BedrockEmbeddings()

# Generate embedding
text = "Software engineer with 5 years of experience in Python"
embedding = embeddings_client.get_embedding(text, dimension=1024)

# Use embedding for vector search or storage
```

### Vector Search

```python
from retrieve_jd_matches import ResumeRetriever

# Initialize retriever
retriever = ResumeRetriever()

# Perform vector search
results = retriever._vector_search(
    query_text="Senior Java Developer with Spring Boot experience",
    size=20,
    enable_reranking=True
)

# Process results
for result in results:
    print(f"Candidate: {result.get('name')}, Score: {result.get('score')}")
```

## Performance Considerations

- **Text Length**: Performance degrades with very long text inputs
- **Dimension Size**: Higher dimensions provide better semantic fidelity but consume more storage
- **Caching**: Critical for production performance
- **Reranking**: Adds computational overhead but significantly improves relevance
- **OpenSearch Configuration**: Properly configured parameters are essential for search performance

## Advanced Tuning

For production deployments, consider tuning:

1. **Embedding Dimension**: Balance between semantic richness and storage cost
2. **ef_search Parameter**: Higher values improve search accuracy but reduce performance
3. **Query Structuring**: Format job descriptions to emphasize key requirements
4. **Vector Weights**: Adjust the weight of vector scores in reranking
5. **Caching Policy**: Tune cache size and expiration based on usage patterns

## Future Improvements

Potential enhancements to the embedding framework include:

1. **Fine-Tuned Domain Embeddings**: Train specialized embeddings for technical resumes
2. **Multi-Vector Representations**: Store multiple vectors for different aspects of a resume
3. **Contextual Embeddings**: Generate embeddings with domain awareness
4. **Embedding Ensemble**: Combine results from multiple embedding models
5. **Incremental Updates**: Efficient updates to embeddings without full regeneration

## References

- [AWS Bedrock Documentation](https://aws.amazon.com/bedrock/)
- [OpenSearch kNN Documentation](https://opensearch.org/docs/latest/search-plugins/knn/index/)
- [HNSW Algorithm Paper](https://arxiv.org/abs/1603.09320) 