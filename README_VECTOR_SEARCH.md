# Vector Search Improvements

This document outlines the improvements made to the resume matching system to enhance vector search performance.

## Key Issues Identified

1. **Embedding Generation Misalignment**: Resume and job description texts were processed differently, creating embeddings that didn't align well semantically.

2. **Dimension Reduction**: The system was using 512-dimension embeddings padded with zeros to reach 1024 dimensions, reducing semantic information.

3. **Score Normalization**: The score multiplier (10x) wasn't sufficient to properly normalize vector similarity scores.

4. **Weight Distribution**: Vector search scores weren't given enough weight in the combined ranking.

5. **Skill Matching**: The exact string matching approach for skills didn't account for variations and abbreviations.

## Implemented Solutions

### 1. Standardized Text Representation

- Created a common template format that's used for both resumes and job descriptions
- Ensures both document types have the same structure and semantic organization
- Implemented in `create_standardized_text()` in `src/utils/bedrock_embeddings.py`

```python
COMMON_TEMPLATE = """
SKILLS: {skills}
EXPERIENCE: {experience}
POSITIONS: {positions}
EDUCATION: {education}
TECHNOLOGIES: {technologies}
SUMMARY: {summary}
"""
```

### 2. Full-Dimension Embeddings

- Removed dimension reduction, now using full 1024-dimension embeddings
- Removed zero padding which was distorting vector similarity calculations
- Updated `get_embedding()` method to consistently use 1024 dimensions

### 3. Improved Score Normalization

- Increased the score multiplier from 10x to 12x based on testing
- Added min/max bounds to ensure scores stay within 0-100 range
- Added raw score debugging information

```python
# Apply stronger normalization (12x multiplier as recommended)
normalized_score = min(raw_score * 12, 100)
```

### 4. Optimized Weight Distribution

- Increased vector score weight from 50% to 60%
- Reduced skill match weight from 30% to 25%
- Reduced experience match weight from 20% to 15%

```python
combined_score = (
    normalized_search_score * 0.60 +  # 60% weight to vector score
    skill_score * 0.25 +              # 25% weight to skill match
    exp_score * 0.15                  # 15% weight to experience match
)
```

### 5. Enhanced Skill Matching

- Added skill normalization to handle variations and abbreviations
- Implemented partial matching for skills (substring matching)
- Created a comprehensive skill mapping dictionary

```python
# Normalize skills with variations handling
resume_skills_norm = [self.normalize_skill(skill) for skill in resume_skills]
jd_skills_norm = [self.normalize_skill(skill) for skill in jd_skills]
```

### 6. JD Text Processing

- Added automatic extraction of structured information from job descriptions
- Implemented regex patterns to extract skills, experience, and job titles
- Created a more focused query representation for vector search

### 7. Debugging Tools

- Created a debug script (`debug_similarity.py`) to test vector similarity
- Added raw score display for better troubleshooting
- Implemented cosine similarity calculation for direct comparison

## Expected Outcomes

With these improvements, the system should now:

1. Provide much higher vector similarity scores for matching resumes
2. Better handle variations in skill names and abbreviations
3. Give more weight to semantic similarity over exact keyword matching
4. Produce more accurate and relevant search results

## Usage

Use the standard retrieve_jd_matches.py script with the `--method vector` parameter (now the default):

```bash
python retrieve_jd_matches.py --jd_file data/sample_jd.txt --method vector
```

For debugging vector similarity between a specific resume and job description:

```bash
python debug_similarity.py --resume [RESUME_ID] --jd data/sample_jd.txt
``` 