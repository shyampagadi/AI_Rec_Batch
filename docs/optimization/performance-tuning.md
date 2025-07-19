# Performance Tuning

This guide provides recommendations for optimizing the performance of the Resume Parser & Matching System in various deployment scenarios.

## Processing Performance

### Resume Parsing Optimization

#### Batch Processing

When processing large numbers of resumes:

1. **Increase batch size**:
   ```bash
   python parse_resume.py --batch-size 50
   ```

2. **Use multiprocessing**:
   ```bash
   python parse_resume.py --workers 4
   ```

3. **Pipeline optimization**:
   ```bash
   python parse_resume.py --pipeline fast
   ```

#### OCR Performance

For image-based PDFs requiring OCR:

1. **Reduce resolution** for faster processing (with slight quality trade-off):
   ```bash
   python parse_resume.py --ocr-dpi 200
   ```

2. **Set page limits** to process only the first N pages of very long documents:
   ```bash
   python parse_resume.py --max-pages 10
   ```

3. **GPU acceleration** when available:
   ```bash
   python parse_resume.py --use-gpu
   ```

#### Memory Optimization

To reduce memory usage during processing:

1. **Stream processing** for large files:
   ```bash
   python parse_resume.py --stream
   ```

2. **Reduce embedding dimension** (trade-off with accuracy):
   ```bash
   python parse_resume.py --embedding-dim 768
   ```

3. **Clean temporary files** aggressively:
   ```bash
   python parse_resume.py --clean-temp
   ```

## Search Performance

### OpenSearch Optimization

1. **Index optimization** for faster vector search:
   ```bash
   python scripts/optimize_index.py
   ```

2. **Adjust HNSW parameters** for speed vs. accuracy trade-off:
   ```
   # In .env file
   OPENSEARCH_EF_SEARCH=128  # Lower = faster, less accurate
   OPENSEARCH_M=16           # Lower = less memory, slower
   ```

3. **Use index sharding** for large resume collections:
   ```bash
   python scripts/reindex_with_shards.py --shards 3
   ```

### Query Optimization

1. **Reduce result size** for faster queries:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --max 10
   ```

2. **Disable reranking** for initial quick searches:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --no-rerank
   ```

3. **Use query caching** for repeated searches:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --cache
   ```

## Scaling Considerations

### Horizontal Scaling

For high-volume resume processing:

1. **Distributed processing** with queue-based architecture:
   ```bash
   # Worker node startup
   python worker.py --queue resume-processing-queue
   ```

2. **Separate parsing and embedding** processes:
   ```bash
   python parse_text.py   # Step 1: Parse text only
   python create_embeddings.py  # Step 2: Generate embeddings
   ```

3. **DB connection pooling** for shared database access:
   ```
   # In .env file
   DB_POOL_SIZE=10
   DB_MAX_OVERFLOW=20
   ```

### AWS Resource Optimization

1. **OpenSearch instance sizing**:

   | Resume Count | Instance Type | Number |
   |--------------|--------------|--------|
   | <10,000      | t3.medium    | 3      |
   | 10k-100k     | r6g.large    | 3      |
   | >100k        | r6g.xlarge   | 5+     |

2. **S3 Transfer Acceleration** for faster uploads:
   ```
   # In .env file
   S3_ACCELERATE=True
   ```

3. **Regional optimization** - ensure all AWS services are in same region:
   ```
   # In .env file
   AWS_REGION=us-east-1  # Use the same region for all services
   ```

## Caching Strategy

### Implement Multi-level Caching

1. **Embedding cache** to avoid regenerating embeddings:
   ```python
   # Set larger cache size in .env
   EMBEDDING_CACHE_SIZE=2000  # Number of embeddings to keep in memory
   ```

2. **Search result cache** for frequent queries:
   ```python
   # Set cache TTL in .env
   SEARCH_CACHE_TTL=3600  # Results valid for 1 hour
   ```

3. **Structured data cache** for parsed resume fields:
   ```python
   # Enable Redis caching in .env
   ENABLE_REDIS_CACHE=True
   REDIS_URL=redis://localhost:6379/0
   ```

## Monitoring and Profiling

### Performance Monitoring

1. **Enable metrics collection**:
   ```bash
   python parse_resume.py --metrics
   ```

2. **Generate performance report**:
   ```bash
   python scripts/performance_report.py --days 7
   ```

3. **Log slow operations** for targeted optimization:
   ```
   # In .env file
   LOG_SLOW_OPERATIONS=True
   SLOW_OPERATION_THRESHOLD_MS=500
   ```

### Profiling Tools

1. **Basic profiling** of resume processing:
   ```bash
   python -m cProfile -o profile.stats parse_resume.py --file resume.pdf
   python scripts/analyze_profile.py --stats profile.stats
   ```

2. **Memory profiling** for large workloads:
   ```bash
   python -m memory_profiler parse_resume.py --file large_resume.pdf
   ```

3. **Query profiling** for search optimization:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --profile
   ```

## Configuration Tuning Recommendations

### Small Deployment (<1,000 resumes)

```ini
# Recommended .env settings
EMBEDDING_CACHE_SIZE=500
BATCH_SIZE=10
WORKERS=2
OPENSEARCH_REPLICAS=0
OPENSEARCH_SHARDS=1
```

### Medium Deployment (1,000-50,000 resumes)

```ini
# Recommended .env settings
EMBEDDING_CACHE_SIZE=2000
BATCH_SIZE=50
WORKERS=4
OPENSEARCH_REPLICAS=1
OPENSEARCH_SHARDS=3
ENABLE_REDIS_CACHE=True
```

### Large Deployment (>50,000 resumes)

```ini
# Recommended .env settings
EMBEDDING_CACHE_SIZE=5000
BATCH_SIZE=100
WORKERS=8
OPENSEARCH_REPLICAS=2
OPENSEARCH_SHARDS=5
ENABLE_REDIS_CACHE=True
ENABLE_DISTRIBUTED_PROCESSING=True
```

## Testing Methodology

### Benchmark Suite

1. **Run standard benchmark**:
   ```bash
   python scripts/benchmark.py
   ```

2. **Performance test with synthetic data**:
   ```bash
   python scripts/generate_test_data.py --count 1000
   python scripts/performance_test.py --dataset synthetic
   ```

3. **Comparative analysis** of configuration options:
   ```bash
   python scripts/compare_configs.py --base baseline.conf --test optimized.conf
   ```

## Optimization Checklist

Use this checklist when tuning your deployment:

1. ✅ Right-size OpenSearch instances based on resume volume
2. ✅ Adjust batch processing parameters based on available hardware
3. ✅ Configure caching based on query patterns and memory availability
4. ✅ Set appropriate HNSW parameters for your accuracy/speed needs
5. ✅ Monitor slow operations and optimize bottlenecks
6. ✅ Consider distributed processing for high-volume environments
7. ✅ Benchmark before and after optimization changes 