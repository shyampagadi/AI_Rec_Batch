# Search Parameter Tuning

This guide explains how to optimize search parameters in the Resume Parser & Matching System to achieve the best balance between accuracy, relevance, and performance.

## Understanding Search Parameters

The system offers various parameters that can be tuned to improve search results based on your specific requirements. These parameters affect:

1. **Semantic Matching**: How documents are matched based on meaning rather than keywords
2. **Reranking**: How initial search results are refined and reordered 
3. **Performance**: How quickly searches are processed

## Vector Search Parameters

### Key OpenSearch kNN Parameters

The following parameters can be configured in the `.env` file or passed at query time:

| Parameter | Description | Default | Range | Impact |
|-----------|-------------|---------|-------|--------|
| `ef_search` | Accuracy vs. speed trade-off | 512 | 50-1000 | Higher = more accurate but slower |
| `num_candidates` | Candidates per shard | 200 | 100-1000 | Higher = more accurate but slower |
| `k` | Number of neighbors | 100 | 20-1000 | Match pool size |

Example of custom query parameters:

```python
# Prioritize accuracy
accuracy_params = {
    "knn": {
        "field": "resume_embedding",
        "query_vector": embedding,
        "k": 200,
        "num_candidates": 500,
        "ef_search": 800
    }
}

# Prioritize speed
speed_params = {
    "knn": {
        "field": "resume_embedding",
        "query_vector": embedding,
        "k": 50,
        "num_candidates": 100,
        "ef_search": 100
    }
}
```

### Fine-Tuning Vector Search

1. **Adjust accuracy/speed trade-off**:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --method vector --ef_search 800
   ```

2. **Change the number of initial candidates**:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --method vector --candidates 300
   ```

3. **Set initial retrieval size** (before reranking):
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --method vector --initial_size 200
   ```

## Reranking Parameters

### Component Weight Tuning

The reranking algorithm uses a weighted combination of multiple scoring components:

| Component | Default Weight | Range | When to Increase |
|-----------|---------------|-------|-----------------|
| `vector` | 0.50 | 0.1-0.9 | For semantic matching |
| `skill` | 0.25 | 0.1-0.5 | For exact skill matching |
| `experience` | 0.15 | 0.0-0.3 | For experience requirements |
| `recency` | 0.10 | 0.0-0.2 | For recent work prioritization |

Example of custom weights:

```bash
# Prioritize skills
python retrieve_jd_matches.py --jd_file job.txt --weights "vector=0.4,skill=0.4,experience=0.15,recency=0.05"

# Prioritize experience level
python retrieve_jd_matches.py --jd_file job.txt --weights "vector=0.4,skill=0.25,experience=0.3,recency=0.05"

# Pure semantic matching
python retrieve_jd_matches.py --jd_file job.txt --weights "vector=1.0,skill=0.0,experience=0.0,recency=0.0"
```

### Experience Requirements

Fine-tune experience requirements for better matching:

```bash
# Set minimum experience
python retrieve_jd_matches.py --jd_file job.txt --exp 5

# Set minimum and preferred experience
python retrieve_jd_matches.py --jd_file job.txt --exp 3 --preferred_exp 7
```

### Score Thresholds

Adjust score thresholds to filter out lower-quality matches:

```bash
# Set minimum overall score
python retrieve_jd_matches.py --jd_file job.txt --min_score 70

# Set component-specific thresholds
python retrieve_jd_matches.py --jd_file job.txt --min_vector_score 65 --min_skill_score 50
```

## Search Method Selection

### Choosing the Right Search Method

The system offers three search methods:

1. **Vector search** (default):
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --method vector
   ```
   - Best for: Semantic understanding, finding candidates with relevant experience even if terminology differs
   - When to use: Most general-purpose searches, when looking for overall fit

2. **Text search**:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --method text
   ```
   - Best for: Exact keyword matching, specific technical requirements
   - When to use: When specific skills or certifications are absolute requirements

3. **Hybrid search**:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --method hybrid
   ```
   - Best for: Balancing semantic and keyword matching
   - When to use: When both meaning and specific terminology matter

### Hybrid Search Configuration

Fine-tune hybrid search parameters:

```bash
# Adjust the balance between vector and text search
python retrieve_jd_matches.py --jd_file job.txt --method hybrid --hybrid_ratio 0.7
```

Where `hybrid_ratio` is the weight given to vector search results (0.0-1.0).

## Parameter Tuning by Job Type

### Technical Roles

For software engineering, data science, and other technical roles:

```bash
python retrieve_jd_matches.py --jd_file technical_role.txt \
  --method hybrid \
  --weights "vector=0.4,skill=0.4,experience=0.15,recency=0.05" \
  --hybrid_ratio 0.6
```

Key considerations:
- Higher weight on skills to ensure technical requirements are met
- Hybrid search to balance semantic understanding with exact skill matching
- Moderate experience weighting

### Management Roles

For management, leadership, and executive positions:

```bash
python retrieve_jd_matches.py --jd_file management_role.txt \
  --method vector \
  --weights "vector=0.6,skill=0.15,experience=0.15,recency=0.1" \
  --exp 8
```

Key considerations:
- Higher vector weight to capture leadership qualities and overall experience
- Lower skill weight (specific technologies less important)
- Higher experience requirement
- Pure vector search for better semantic understanding

### Entry-Level Positions

For junior roles and entry-level positions:

```bash
python retrieve_jd_matches.py --jd_file junior_role.txt \
  --method vector \
  --weights "vector=0.7,skill=0.25,experience=0.0,recency=0.05" \
  --exp 0
```

Key considerations:
- Zero or low experience requirement
- Higher vector weight to focus on potential and education
- No reranking based on experience

## Performance vs. Accuracy Optimization

### High-Performance Configuration

For quick searches and large candidate pools:

```bash
python retrieve_jd_matches.py --jd_file job.txt \
  --method vector \
  --max 10 \
  --ef_search 100 \
  --candidates 100 \
  --no-rerank
```

Key settings:
- Lower `ef_search` and `candidates` values
- Disable reranking for raw vector search
- Limit results to top 10

### High-Accuracy Configuration

For thorough candidate evaluation:

```bash
python retrieve_jd_matches.py --jd_file job.txt \
  --method hybrid \
  --max 50 \
  --ef_search 800 \
  --candidates 500 \
  --weights "vector=0.4,skill=0.3,experience=0.2,recency=0.1"
```

Key settings:
- Higher `ef_search` and `candidates` values
- Hybrid search method
- Balanced reranking weights
- Larger result set

## Experimental Tuning Process

### A/B Testing Parameters

To systematically find optimal parameters:

1. **Create baseline search**:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --output baseline.json
   ```

2. **Create variant with different parameters**:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --output variant1.json --weights "vector=0.4,skill=0.4,experience=0.1,recency=0.1"
   ```

3. **Compare results**:
   ```bash
   python scripts/compare_results.py --baseline baseline.json --variant variant1.json
   ```

### Parameter Grid Search

For automated parameter optimization:

```bash
python scripts/grid_search.py \
  --jd_file job.txt \
  --vector_weights 0.4 0.5 0.6 \
  --skill_weights 0.2 0.3 0.4 \
  --exp_weights 0.1 0.2 \
  --recency_weights 0.05 0.1
```

This will test all combinations of parameters and report the best configuration based on a quality metric.

## Debugging Search Results

### Analyzing Match Failures

If search results are unsatisfactory:

1. **Get detailed scoring information**:
   ```bash
   python retrieve_jd_matches.py --jd_file job.txt --debug
   ```

2. **Analyze individual component scores**:
   ```bash
   python scripts/analyze_scores.py --results job_matches_20250526_231134.json
   ```

3. **Compare search methods**:
   ```bash
   python scripts/compare_methods.py --jd_file job.txt
   ```

### Troubleshooting Common Issues

1. **No relevant results**:
   - Verify vector quality with similarity test:
     ```bash
     python scripts/test_similarity.py --text1 "sample jd text" --text2 "sample resume text"
     ```
   - Try lowering thresholds:
     ```bash
     python retrieve_jd_matches.py --jd_file job.txt --min_score 30
     ```

2. **Results missing expected skills**:
   - Increase skill weight:
     ```bash
     python retrieve_jd_matches.py --jd_file job.txt --weights "vector=0.3,skill=0.5,experience=0.1,recency=0.1"
     ```
   - Use hybrid search:
     ```bash
     python retrieve_jd_matches.py --jd_file job.txt --method hybrid
     ```

3. **Slow search performance**:
   - Reduce parameter values:
     ```bash
     python retrieve_jd_matches.py --jd_file job.txt --ef_search 100 --candidates 100
     ```
   - Disable reranking for initial tests:
     ```bash
     python retrieve_jd_matches.py --jd_file job.txt --no-rerank
     ```

## Best Practices Summary

### General Recommendations

- **Start with defaults**: Begin with default parameters and adjust based on results
- **Systematic tuning**: Change one parameter at a time to understand its impact
- **Job-specific optimization**: Customize parameters based on the role type
- **Regular re-evaluation**: Periodically review and adjust parameters as your candidate pool grows

### Parameter Selection Cheatsheet

| Scenario | Vector Weight | Skill Weight | Experience Weight | Recency Weight | Search Method |
|----------|--------------|--------------|------------------|---------------|--------------|
| Technical roles | 0.4 | 0.4 | 0.15 | 0.05 | Hybrid |
| Management roles | 0.6 | 0.15 | 0.15 | 0.1 | Vector |
| Entry-level | 0.7 | 0.25 | 0.0 | 0.05 | Vector |
| Specific skills required | 0.3 | 0.5 | 0.1 | 0.1 | Hybrid |
| General fit | 0.6 | 0.2 | 0.1 | 0.1 | Vector |
| Quick screening | 0.5 | 0.3 | 0.2 | 0.0 | Vector + No rerank |
| Detailed evaluation | 0.4 | 0.3 | 0.2 | 0.1 | Hybrid |

### Configuration Templates

Create configuration templates for different search scenarios:

```bash
# Save current configuration
python scripts/save_config.py --name "technical_roles" --current

# Load a configuration
python scripts/load_config.py --name "technical_roles"

# View available configurations
python scripts/list_configs.py
```

This allows you to quickly switch between optimized parameter sets for different hiring scenarios.

## Advanced Parameter Optimization

### Seasonality and Trend Adaptation

Adjust parameters based on market trends:

```bash
python scripts/trend_optimizer.py --analyze-market --adjust-params
```

This will analyze recent matches and suggest parameter adjustments based on changing market conditions.

### Feedback-Based Tuning

Incorporate recruiter feedback to refine parameters:

```bash
python scripts/feedback_optimizer.py --input feedback.csv
```

Where `feedback.csv` contains structured feedback on match quality that can be used to refine parameters for future searches. 