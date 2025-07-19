# Using Job Description Matching

This guide provides detailed instructions for using the Job Description (JD) matching feature of the Resume Parser & Matching System.

## Overview

The JD matching functionality enables you to:

1. Find the most relevant candidates for a job description
2. Rank candidates based on skill match, experience, and semantic relevance
3. Customize search parameters to meet specific recruitment needs
4. Generate structured reports of matching candidates

## Prerequisites

Before using JD matching:

- Ensure you've [installed the system](../installation/installation-guide.md)
- Parsed and indexed resumes using `parse_resume.py` (see [Using Resume Parser](./using-resume-parser.md))
- Created a job description file (.txt format)

## Job Description Format

For optimal results, structure your job descriptions with clear sections:

```
Job Title: Senior Java Developer

Required Skills:
- Java
- Spring Boot
- Microservices
- RESTful APIs
- SQL

Experience: 5+ years of Java development

Responsibilities:
- Design, develop and maintain Java applications
- Work with the team to establish specifications
- Troubleshoot and debug applications
- Implement security and data protection

Qualifications:
- Bachelor's degree in Computer Science or related field
- Experience with CI/CD pipelines
- Knowledge of cloud platforms (AWS/Azure)
```

While the system can process unstructured job descriptions, clearly labeled sections improve matching accuracy.

## Basic Usage

### Command Line Interface

The basic command to match a job description with resumes:

```bash
python retrieve_jd_matches.py --jd_file path/to/job_description.txt
```

### Available Options

```bash
python retrieve_jd_matches.py --help
```

Key options include:

- `--jd_file`: Path to job description file (required)
- `--method`: Search method (vector, text, hybrid) - default is "vector"
- `--max`: Maximum number of results to return (default: 20)
- `--exp`: Required years of experience (default: 3.0)
- `--no-rerank`: Disable reranking algorithm (use pure vector similarity)
- `--output`: Custom output file path (default: auto-generated with timestamp)
- `--verbose`: Enable verbose output
- `--weights`: Custom weights for reranking components (advanced)

## Search Methods

### Vector Search (Default)

```bash
python retrieve_jd_matches.py --jd_file job_description.txt --method vector
```

Vector search uses semantic embeddings to find candidates based on the meaning of the job description, not just keywords. This method is best for:
- Finding candidates with relevant experience, even if terminology differs
- Identifying transferable skills from adjacent domains
- Capturing the overall context of requirements

### Text Search

```bash
python retrieve_jd_matches.py --jd_file job_description.txt --method text
```

Text search uses traditional keyword matching. This method is useful for:
- Finding candidates with very specific technical skills
- Exact keyword matching for compliance or certification requirements
- Simple searches with minimal setup (doesn't require vector embeddings)

### Hybrid Search

```bash
python retrieve_jd_matches.py --jd_file job_description.txt --method hybrid
```

Hybrid search combines both vector and text search. It's ideal for:
- Balancing semantic understanding with keyword precision
- Complex roles requiring both specific skills and broader domain knowledge
- Maximum recall for a wider candidate pool

## Advanced Usage

### Customizing Experience Requirements

Specify minimum years of experience:

```bash
python retrieve_jd_matches.py --jd_file job_description.txt --exp 5.0
```

This will:
1. Filter for candidates with 5+ years of experience
2. Adjust scoring to favor candidates meeting this threshold
3. Still show some candidates below this threshold (but with lower ranking)

### Disabling Reranking

For pure vector similarity without additional scoring factors:

```bash
python retrieve_jd_matches.py --jd_file job_description.txt --no-rerank
```

This bypasses the reranking algorithm, using only the raw vector similarity score. Useful for:
- Benchmarking the base vector search performance
- Finding semantically similar resumes regardless of specific skills
- Simplified scoring for general exploratory searches

### Custom Scoring Weights

Advanced users can customize the weight of each scoring component:

```bash
python retrieve_jd_matches.py --jd_file job_description.txt --weights "vector=0.4,skill=0.3,experience=0.2,recency=0.1"
```

The weights must sum to 1.0 and include these components:
- `vector`: Semantic similarity from vector embeddings (default: 0.5)
- `skill`: Explicit skill matching score (default: 0.25)
- `experience`: Years of experience match (default: 0.15)
- `recency`: Recency of relevant experience (default: 0.1)

### Custom Output Location

Specify a custom output file path:

```bash
python retrieve_jd_matches.py --jd_file job_description.txt --output results/java_developer_matches.json
```

## Understanding Results

The output JSON file contains structured results with several sections:

### Job Description Analysis

```json
{
  "job_description": {
    "content": "Job Title: Senior Java Developer...",
    "skills": ["Java", "Spring Boot", "Microservices", "REST", "SQL"],
    "required_experience": 5.0,
    "embedding_id": "jd_20230928_123456"
  }
}
```

### Candidate Matches

```json
{
  "matches": [
    {
      "resume_id": "resume_123456",
      "name": "John Doe",
      "score": 87.6,
      "vector_score": 83.2,
      "skill_score": 90.0,
      "experience_score": 95.0,
      "recency_score": 85.0,
      "years_of_experience": 6.5,
      "email": "john.doe@example.com",
      "phone": "+1-234-567-8900",
      "matched_skills": ["Java", "Spring Boot", "REST", "SQL"],
      "missing_skills": ["Microservices"],
      "current_position": "Senior Software Engineer at XYZ Corp"
    },
    // Additional candidates...
  ]
}
```

For more details on interpreting results, see [Understanding Results](./understanding-results.md).

## Best Practices

### Optimizing Job Descriptions

For the best matching results:

1. **Be specific about required skills**
   - List exact technologies, frameworks, and tools
   - Differentiate between required and nice-to-have skills

2. **Clearly state experience requirements**
   - Mention specific years of experience
   - Note experience with particular domains/industries

3. **Include key responsibilities**
   - Describe the actual work the candidate will perform
   - This helps semantic matching identify relevant experience

4. **Avoid overly generic terms**
   - "Good communication skills" won't help much with matching
   - Be specific about technical and domain expertise

### Iterative Refinement

Finding the perfect candidate often requires iterative searches:

1. Start with a broad search to see the candidate pool
2. Refine the job description to target specific skills
3. Adjust experience requirements if needed
4. Try different search methods for comparison
5. Tweak reranking weights for your specific priorities

## Example Scenarios

### Scenario 1: Finding Technical Specialists

```bash
python retrieve_jd_matches.py --jd_file specialists/ml_engineer.txt --method vector --exp 4.0 --weights "vector=0.4,skill=0.4,experience=0.1,recency=0.1"
```

This configuration:
- Increases weight on specific skills for technical specialist roles
- Sets higher experience threshold for specialized positions
- Maintains vector search for semantic understanding

### Scenario 2: Entry-Level Positions

```bash
python retrieve_jd_matches.py --jd_file entry_level/junior_developer.txt --exp 0.0 --weights "vector=0.7,skill=0.2,experience=0.0,recency=0.1"
```

This configuration:
- Removes experience requirements for entry-level positions
- Increases emphasis on semantic matching to identify potential
- Reduces weight on experience metrics

### Scenario 3: Leadership Roles

```bash
python retrieve_jd_matches.py --jd_file leadership/tech_director.txt --exp 8.0 --method hybrid
```

This configuration:
- Uses hybrid search to balance keywords and semantic meaning
- Sets high experience threshold for leadership roles
- Default weights to balance various factors

## Troubleshooting

### No Results Found

If your search returns no results:

1. Check if experience requirements are too restrictive
2. Verify that skills listed in the job description are common in the resume database
3. Try a different search method (e.g., try text search instead of vector)
4. Review the job description for unusual terminology

### Poor Quality Matches

If matches don't seem relevant:

1. Make sure resumes have been properly parsed and indexed
2. Add more specific skills and requirements to the job description
3. Try adjusting the reranking weights
4. Check if the job description is clear and well-structured

### Performance Issues

For slow searches:

1. Reduce the maximum number of results (`--max`)
2. Verify that OpenSearch is properly configured
3. Consider optimizing the OpenSearch domain settings

## Next Steps

After finding matching candidates:

1. Review candidate profiles in detail
2. Consider running additional searches with refined parameters
3. Export results for sharing with the hiring team
4. Schedule interviews with top matches

For more detailed information, see:
- [Understanding Results](./understanding-results.md)
- [Reranking Algorithm](../technical-docs/reranking-algorithm.md)
- [Vector Embedding Framework](../technical-docs/vector-embedding-framework.md) 