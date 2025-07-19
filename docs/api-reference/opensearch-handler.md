# OpenSearch Handler API Reference

This document provides reference documentation for the OpenSearch handler component of the Resume Parser & Matching System.

## Overview

The OpenSearch handler manages all interactions with the Amazon OpenSearch Service, providing vector search capabilities, document storage, and index management.

## Class Reference

### `OpenSearchHandler`

```python
class OpenSearchHandler:
    """Handles interactions with OpenSearch for resume storage and retrieval"""

    def __init__(self, host=None, region=None, index_name=None, use_iam_auth=True):
        """
        Initialize OpenSearch connection.
        
        Args:
            host (str, optional): OpenSearch domain endpoint. Defaults to env var OPENSEARCH_HOST.
            region (str, optional): AWS region. Defaults to env var AWS_REGION.
            index_name (str, optional): Index name. Defaults to env var OPENSEARCH_INDEX_NAME or "resumes".
            use_iam_auth (bool, optional): Whether to use IAM authentication. Defaults to True.
        """
```

## Core Methods

### Index Management

#### `create_index`

```python
def create_index(self, force=False):
    """
    Create the OpenSearch index with appropriate mappings.
    
    Args:
        force (bool): If True, delete existing index before creation
        
    Returns:
        bool: True if index was created, False if it already existed
    
    Raises:
        OpenSearchException: If index creation fails
    """
```

#### `delete_index`

```python
def delete_index(self):
    """
    Delete the OpenSearch index.
    
    Returns:
        bool: True if index was deleted, False if it didn't exist
    """
```

#### `check_index_exists`

```python
def check_index_exists(self):
    """
    Check if the index exists.
    
    Returns:
        bool: True if index exists, False otherwise
    """
```

### Document Operations

#### `index_document`

```python
def index_document(self, doc_id, document, refresh=True):
    """
    Index a document in OpenSearch.
    
    Args:
        doc_id (str): Document ID
        document (dict): Document body
        refresh (bool): Whether to refresh the index
        
    Returns:
        dict: OpenSearch response
        
    Raises:
        OpenSearchException: If indexing fails
    """
```

#### `bulk_index`

```python
def bulk_index(self, documents, refresh=True):
    """
    Index multiple documents in OpenSearch with bulk API.
    
    Args:
        documents (list): List of (id, document) tuples
        refresh (bool): Whether to refresh the index
        
    Returns:
        dict: OpenSearch bulk response
        
    Raises:
        OpenSearchException: If bulk indexing fails
    """
```

#### `get_document`

```python
def get_document(self, doc_id):
    """
    Retrieve a document by ID.
    
    Args:
        doc_id (str): Document ID
        
    Returns:
        dict: Document data or None if not found
    """
```

#### `delete_document`

```python
def delete_document(self, doc_id, refresh=True):
    """
    Delete a document by ID.
    
    Args:
        doc_id (str): Document ID
        refresh (bool): Whether to refresh the index
        
    Returns:
        bool: True if document was deleted, False if not found
    """
```

### Vector Search

#### `vector_search`

```python
def vector_search(self, 
                 vector, 
                 size=10, 
                 filters=None, 
                 ef_search=None, 
                 include_vector=False):
    """
    Perform a vector similarity search in OpenSearch.
    
    Args:
        vector (list): Query vector
        size (int): Number of results to return
        filters (dict): OpenSearch filters to apply
        ef_search (int): ef_search parameter for accuracy vs. speed trade-off
        include_vector (bool): Whether to include vectors in results
        
    Returns:
        list: List of matching documents with scores
        
    Raises:
        OpenSearchException: If search fails
    """
```

#### `hybrid_search`

```python
def hybrid_search(self, 
                 vector, 
                 text_query, 
                 size=10, 
                 vector_weight=0.7,
                 text_weight=0.3,
                 fields=None):
    """
    Perform a hybrid search combining vector and text matches.
    
    Args:
        vector (list): Query vector
        text_query (str): Text query
        size (int): Number of results to return
        vector_weight (float): Weight for vector results (0.0-1.0)
        text_weight (float): Weight for text results (0.0-1.0)
        fields (list): Fields to search for text query
        
    Returns:
        list: List of matching documents with combined scores
        
    Raises:
        OpenSearchException: If search fails
    """
```

#### `text_search`

```python
def text_search(self, query, fields=None, size=10, filters=None):
    """
    Perform a text search in OpenSearch.
    
    Args:
        query (str): Text query
        fields (list): Fields to search
        size (int): Number of results to return
        filters (dict): OpenSearch filters to apply
        
    Returns:
        list: List of matching documents with scores
        
    Raises:
        OpenSearchException: If search fails
    """
```

## Index Configuration

### Default Index Mapping

```python
DEFAULT_INDEX_MAPPING = {
    "settings": {
        "index": {
            "knn": True,
            "knn.algo_param.ef_search": 512,
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
                        "ef_construction": 512,
                        "m": 16
                    }
                }
            },
            "full_text": {
                "type": "text"
            },
            "skills": {
                "type": "keyword",
                "normalizer": "lowercase"
            },
            "years_of_experience": {
                "type": "float"
            },
            "positions": {
                "type": "nested",
                "properties": {
                    "title": {"type": "text"},
                    "company": {"type": "text"},
                    "start_date": {"type": "date", "format": "yyyy-MM||yyyy-MM-dd||yyyy"},
                    "end_date": {"type": "date", "format": "yyyy-MM||yyyy-MM-dd||yyyy||present"},
                    "description": {"type": "text"}
                }
            },
            "education": {
                "type": "nested",
                "properties": {
                    "degree": {"type": "text"},
                    "institution": {"type": "text"},
                    "graduation_date": {"type": "date", "format": "yyyy-MM||yyyy-MM-dd||yyyy"}
                }
            },
            "metadata": {
                "type": "object",
                "enabled": False
            }
        }
    }
}
```

## Utility Methods

#### `get_index_stats`

```python
def get_index_stats(self):
    """
    Get statistics about the OpenSearch index.
    
    Returns:
        dict: Index statistics
    """
```

#### `refresh_index`

```python
def refresh_index(self):
    """
    Refresh the OpenSearch index.
    
    Returns:
        dict: OpenSearch response
    """
```

#### `update_settings`

```python
def update_settings(self, settings):
    """
    Update index settings.
    
    Args:
        settings (dict): Settings to update
        
    Returns:
        dict: OpenSearch response
        
    Raises:
        OpenSearchException: If update fails
    """
```

## Exception Handling

The OpenSearchHandler wraps all OpenSearch exceptions to provide clearer error messages:

```python
try:
    result = self.opensearch.index(
        index=self.index_name,
        body=document,
        id=doc_id,
        refresh=refresh
    )
    return result
except OpenSearchException as e:
    logging.error(f"Error indexing document {doc_id}: {str(e)}")
    raise OpenSearchException(f"Failed to index document: {str(e)}")
```

## Configuration Options

The OpenSearchHandler can be configured via environment variables or constructor parameters:

| Parameter | Env Variable | Default | Description |
|-----------|--------------|---------|-------------|
| host | OPENSEARCH_HOST | None (required) | OpenSearch domain endpoint |
| region | AWS_REGION | None (required) | AWS region |
| index_name | OPENSEARCH_INDEX_NAME | "resumes" | Index name |
| use_iam_auth | N/A | True | Whether to use IAM authentication |

## Usage Examples

### Basic Initialization

```python
from src.utils.opensearch_handler import OpenSearchHandler

# Initialize with environment variables
handler = OpenSearchHandler()

# Initialize with explicit parameters
handler = OpenSearchHandler(
    host="https://my-domain.us-east-1.es.amazonaws.com",
    region="us-east-1",
    index_name="resumes"
)
```

### Creating and Managing Index

```python
# Create index if it doesn't exist
handler.create_index()

# Force recreate index (caution: deletes all data)
handler.create_index(force=True)

# Check if index exists
if handler.check_index_exists():
    print("Index exists")

# Get index statistics
stats = handler.get_index_stats()
print(f"Document count: {stats['_all']['primaries']['docs']['count']}")
```

### Indexing Documents

```python
# Index a single document
doc_id = "resume_123"
document = {
    "resume_embedding": embedding_vector,  # 1024-dim vector
    "full_text": "Full resume text...",
    "skills": ["Python", "AWS", "Machine Learning"],
    "years_of_experience": 5.5,
    "positions": [
        {
            "title": "Senior Developer",
            "company": "Tech Corp",
            "start_date": "2019-01",
            "end_date": "present",
            "description": "Led development team..."
        }
    ]
}
handler.index_document(doc_id, document)

# Bulk indexing
documents = [
    ("resume_124", {...}),
    ("resume_125", {...})
]
handler.bulk_index(documents)
```

### Vector Search

```python
# Simple vector search
query_vector = [0.1, 0.2, ...] # 1024-dim vector
results = handler.vector_search(
    vector=query_vector,
    size=20
)

# Vector search with filtering
results = handler.vector_search(
    vector=query_vector,
    size=20,
    filters={
        "bool": {
            "must": [
                {"term": {"skills": "python"}}
            ],
            "filter": [
                {"range": {"years_of_experience": {"gte": 3}}}
            ]
        }
    }
)

# Hybrid search
results = handler.hybrid_search(
    vector=query_vector,
    text_query="machine learning engineer",
    size=20,
    vector_weight=0.8,
    text_weight=0.2,
    fields=["full_text", "positions.description"]
)
```

## Best Practices

1. **Connection Management**
   - Reuse the OpenSearchHandler instance across requests
   - Handle connection errors with appropriate retries

2. **Bulk Operations**
   - Use `bulk_index` for indexing multiple documents
   - Set appropriate batch sizes (recommended: 100-1000 documents per batch)

3. **Search Optimization**
   - Adjust `ef_search` parameter based on accuracy/speed requirements
   - Use filters to narrow search scope before vector similarity
   - Consider hybrid search for balance between semantic and keyword matching

4. **Index Management**
   - Monitor index size and shard distribution
   - Schedule index optimization for large indices
   - Consider using index aliases for zero-downtime reindexing 