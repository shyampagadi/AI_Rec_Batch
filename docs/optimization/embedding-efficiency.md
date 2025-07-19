# Embedding Efficiency

This document provides techniques and best practices for optimizing the generation, storage, and retrieval of vector embeddings in the Resume Parser & Matching System.

## Embedding Generation Optimization

### Model Selection

Choosing the right embedding model is critical for balancing quality against efficiency:

| Model | Dimension | Quality | Speed | Cost |
|-------|-----------|---------|-------|------|
| Titan Embed v2 | 1536 | High | Medium | $ |
| Titan Embed v1 | 1024 | Medium | Fast | $ |
| Cohere Embed | 1024 | High | Slower | $$ |
| Claude 3 Embed | 1024 | Very High | Slowest | $$$ |

Configure your embedding model in the `.env` file:
```
BEDROCK_EMBEDDINGS_MODEL=amazon.titan-embed-text-v2:0
```

### Text Preprocessing

Optimize the text before embedding generation:

1. **Remove irrelevant content**:
   ```python
   # Remove boilerplate content that doesn't add semantic value
   text = remove_boilerplate(text)
   ```

2. **Normalize text formatting**:
   ```python
   # Apply consistent formatting to improve embedding quality
   text = normalize_whitespace(text)
   text = normalize_punctuation(text)
   ```

3. **Set optimal text length**:
   Most embedding models have an optimal input length (usually 512-1024 tokens):
   ```python
   # Truncate or chunk text appropriately
   text_chunks = chunk_text(text, max_tokens=512)
   ```

### Batching Strategies

When processing multiple documents:

1. **Use request batching** to reduce API overhead:
   ```python
   # Process embeddings in batches
   texts = [doc1, doc2, doc3, ..., doc20]
   embeddings = embedding_client.get_embeddings_batch(texts)
   ```

2. **Implement parallel processing**:
   ```python
   # Process multiple embedding requests in parallel
   with ThreadPoolExecutor(max_workers=5) as executor:
       embeddings = list(executor.map(get_embedding, texts))
   ```

3. **Configure batch size** based on your deployment:
   ```
   # In .env file
   EMBEDDING_BATCH_SIZE=20  # Adjust based on API limits and memory
   ```

## Embedding Storage Optimization

### Dimensionality Considerations

1. **Right-size embedding dimensions** for your use case:
   ```python
   # Generate lower-dimensional embeddings for speed/storage trade-off
   embedding = get_embedding(text, dimension=768)  # Instead of 1024/1536
   ```

2. **Dimension reduction techniques** when storing large collections:
   ```python
   # Apply PCA for dimension reduction
   from sklearn.decomposition import PCA
   pca = PCA(n_components=512)
   reduced_embeddings = pca.fit_transform(embeddings)
   ```

3. **Quantization** for compact storage:
   ```python
   # Convert 32-bit float vectors to 8-bit integers
   quantized_vectors = quantize_vectors(embeddings)
   ```

### OpenSearch Index Configuration

1. **Optimize kNN index settings**:
   ```json
   {
     "settings": {
       "index": {
         "knn": true,
         "knn.algo_param.ef_construction": 512,
         "knn.algo_param.m": 16,
         "knn.algo_param.ef_search": 512
       }
     }
   }
   ```

2. **Index lifecycle management** for large collections:
   ```python
   # Configure ILM policy
   ilm_policy = {
     "policy": {
       "phases": {
         "hot": {
           "min_age": "0ms",
           "actions": {
             "set_priority": { "priority": 100 }
           }
         },
         "warm": {
           "min_age": "30d",
           "actions": {
             "readonly": {}
           }
         }
       }
     }
   }
   ```

3. **Sharding strategy**:
   ```
   # Optimal shard count based on document count
   # <100K documents: 1 primary shard
   # 100K-1M documents: 3 primary shards
   # >1M documents: 5+ primary shards
   ```

### Storage Format Options

1. **Vector database options**:
   
   | Database | Pros | Cons | Best For |
   |----------|------|------|----------|
   | OpenSearch | AWS integration, full-text + vector | Resource heavy | Full production |
   | Faiss | Fast, light, local | No cloud native | Dev/testing |
   | Pinecone | Managed, scalable | Extra service | Quick deployment |
   | Redis | In-memory, fast | Persistence challenges | Caching layer |

2. **Storage format for backup/transfer**:
   ```python
   # Compact numpy format
   np.save("embeddings.npy", embeddings_array)
   
   # Columnar format for analytics
   import polars as pl
   df = pl.DataFrame({"id": ids, "embedding": embeddings})
   df.write_parquet("embeddings.parquet")
   ```

3. **Streaming storage** for continuous processing:
   ```python
   # Stream embeddings to storage without loading all in memory
   with open("embeddings.jsonl", "w") as f:
       for doc_id, embedding in process_documents():
           f.write(json.dumps({"id": doc_id, "vector": embedding}) + "\n")
   ```

## Retrieval Optimization

### Query Efficiency

1. **Query preprocessing**:
   ```python
   # Apply the same preprocessing as during indexing
   query_text = preprocess_text(query_text)
   ```

2. **Approximate nearest neighbor search** parameters:
   ```python
   # Trade-off between accuracy and speed
   search_params = {
     "size": 20,
     "knn": {
       "field": "resume_embedding",
       "query_vector": query_embedding,
       "k": 50,  # Fetch more candidates than needed
       "num_candidates": 200  # Higher = more accurate but slower
     }
   }
   ```

3. **Post-search filtering**:
   ```python
   # Combined vector + keyword search for efficiency
   search_params = {
     "size": 20,
     "query": {
       "bool": {
         "must": {
           "match": {"skills": "python java"}
         },
         "filter": {
           "knn": {
             "resume_embedding": {
               "vector": query_embedding,
               "k": 100
             }
           }
         }
       }
     }
   }
   ```

### Caching Strategies

1. **Embedding cache**:
   ```python
   # In-memory LRU cache
   @lru_cache(maxsize=1000)
   def get_cached_embedding(text_hash):
       text = text_lookup[text_hash]
       return bedrock_client.get_embedding(text)
   ```

2. **Query result cache**:
   ```python
   # Cache common searches
   @lru_cache(maxsize=100)
   def cached_search(query_hash, size=20, filters=None):
       query = query_lookup[query_hash]
       return search_engine.search(query, size=size, filters=filters)
   ```

3. **Persistent cache** for frequently used embeddings:
   ```python
   # Using Redis as embedding cache
   def get_or_create_embedding(text):
       text_hash = hashlib.md5(text.encode()).hexdigest()
       cached = redis_client.get(f"emb:{text_hash}")
       if cached:
           return np.frombuffer(cached, dtype=np.float32)
       embedding = bedrock_client.get_embedding(text)
       redis_client.set(f"emb:{text_hash}", embedding.tobytes())
       return embedding
   ```

## Performance Monitoring

### Embedding Metrics

1. **Track embedding generation time**:
   ```python
   def get_embedding_with_metrics(text):
       start = time.time()
       embedding = bedrock_client.get_embedding(text)
       duration_ms = (time.time() - start) * 1000
       metrics_logger.log(
           "embedding_generation", 
           {"duration_ms": duration_ms, "text_length": len(text)}
       )
       return embedding
   ```

2. **Monitor API quota usage**:
   ```python
   # Track API usage
   def log_api_usage(model, tokens):
       metrics_logger.increment(
           "bedrock_api_calls", 
           tags={"model": model, "tokens": tokens}
       )
   ```

3. **Embedding quality metrics**:
   ```python
   # Monitor embedding quality with known test cases
   def check_embedding_quality():
       test_cases = load_test_cases()
       results = evaluate_embeddings(test_cases)
       metrics_logger.gauge("embedding_quality", results["score"])
   ```

### Storage Optimization Monitoring

1. **Index size tracking**:
   ```python
   def monitor_index_size():
       stats = opensearch_client.indices.stats(index="resumes")
       size_bytes = stats["indices"]["resumes"]["total"]["store"]["size_in_bytes"]
       metrics_logger.gauge("index_size_bytes", size_bytes)
   ```

2. **Query performance tracking**:
   ```python
   def search_with_metrics(query, **kwargs):
       start = time.time()
       results = opensearch_client.search(query, **kwargs)
       duration_ms = (time.time() - start) * 1000
       metrics_logger.histogram(
           "search_duration_ms", 
           duration_ms,
           tags={"result_count": len(results["hits"]["hits"])}
       )
       return results
   ```

3. **Cache hit ratio monitoring**:
   ```python
   class MonitoredCache:
       def __init__(self):
           self.hits = 0
           self.misses = 0
           self.cache = {}
           
       def get(self, key):
           if key in self.cache:
               self.hits += 1
               metrics_logger.increment("cache_hits")
               return self.cache[key]
           self.misses += 1
           metrics_logger.increment("cache_misses")
           return None
           
       @property
       def hit_ratio(self):
           total = self.hits + self.misses
           return self.hits / total if total > 0 else 0
   ```

## Best Practices Summary

### Embedding Generation

1. **Choose the right model** for your quality/cost requirements
2. **Standardize text preprocessing** for consistent embeddings
3. **Batch processing** for efficiency with larger workloads
4. **Cache frequently used embeddings** to reduce API calls

### Storage Configuration

1. **Optimize shard count** based on document volume
2. **Configure HNSW parameters** appropriately for your use case
3. **Use dimension reduction** when appropriate for large collections
4. **Implement index lifecycle policies** for long-term maintenance

### Retrieval Optimization

1. **Apply identical preprocessing** to queries and documents
2. **Use approximate nearest neighbor** with appropriate parameters
3. **Combine vector search with filters** for better performance
4. **Cache common search results** to improve response time

### Ongoing Maintenance

1. **Monitor index size and fragmentation**
2. **Track embedding API costs**
3. **Measure and optimize cache hit ratios**
4. **Periodically reindex** with optimized settings 