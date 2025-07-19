#!/usr/bin/env python
import os
import sys
import json
import argparse
import numpy as np
from typing import List, Dict, Any

from src.utils.bedrock_embeddings import create_standardized_text, BedrockEmbeddings
from src.storage.opensearch_handler import OpenSearchHandler
from retrieve_jd_matches import ResumeRetriever

def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """Calculate cosine similarity between two vectors"""
    if not vec1 or not vec2:
        return 0.0
    
    # Convert to numpy arrays
    a = np.array(vec1)
    b = np.array(vec2)
    
    # Calculate cosine similarity
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

def debug_similarity(resume_id: str, jd_file: str):
    """Debug vector similarity between a resume and job description"""
    print(f"\n{'='*50}")
    print(f" DEBUG VECTOR SIMILARITY ".center(50, '='))
    print(f"{'='*50}\n")
    
    # Read job description
    with open(jd_file, 'r', encoding='utf-8') as f:
        jd_text = f.read()
    
    print(f"Job Description: {jd_file}")
    print(f"Resume ID: {resume_id}")
    
    # Create retrievers
    retriever = ResumeRetriever()
    opensearch = OpenSearchHandler()
    embeddings = BedrockEmbeddings()
    
    # Get resume data
    resume_data = retriever.get_resume_details(resume_id)
    if not resume_data:
        print(f"Error: Resume with ID {resume_id} not found")
        return
    
    # Extract resume text
    resume_text = ""
    if 'data' in resume_data and resume_data['data']:
        if 'summary' in resume_data['data']:
            summary = resume_data['data']['summary']
            if isinstance(summary, str):
                resume_text += summary
            elif isinstance(summary, dict) and 'text' in summary:
                resume_text += summary['text']
    
    # Create standardized text for both
    jd_structure = {
        "summary": jd_text,
        "skills": [],
        "positions": [],
        "total_experience": 0,
        "education": []
    }
    
    # Extract JD info
    jd_info = retriever.extract_jd_info_llm(jd_text)
    
    # Update structure with extracted info
    if jd_info:
        if 'job_title' in jd_info:
            jd_structure['positions'] = [jd_info['job_title']]
        if 'required_skills' in jd_info:
            jd_structure['skills'] = jd_info['required_skills']
        if 'required_experience' in jd_info:
            jd_structure['total_experience'] = jd_info['required_experience']
        if 'required_education' in jd_info:
            jd_structure['education'] = [{"degree": jd_info['required_education']}]
    
    # Create standardized texts
    jd_standardized = create_standardized_text(jd_structure)
    resume_standardized = create_standardized_text(resume_data['data'])
    
    print("\nGenerating embeddings...")
    
    # Generate embeddings
    jd_embedding = embeddings.get_embedding(jd_standardized, dimension=1024)
    resume_embedding = embeddings.get_embedding(resume_standardized, dimension=1024)
    
    # Calculate similarity
    similarity = cosine_similarity(jd_embedding, resume_embedding)
    
    print(f"\nResults:")
    print(f"  Raw Cosine Similarity: {similarity:.6f}")
    print(f"  Normalized Score (x12): {min(similarity * 12 * 100, 100):.2f}")
    
    # Check skill match
    skill_score = retriever.calculate_skill_match_score(
        resume_data.get('data', {}).get('skills', []),
        jd_info.get('required_skills', [])
    )
    
    print(f"  Skill Match Score: {skill_score:.2f}")
    
    # Check experience match
    resume_exp = 0
    if 'data' in resume_data and 'total_experience' in resume_data['data']:
        resume_exp = float(resume_data['data']['total_experience'])
    
    jd_exp = jd_info.get('required_experience', 0)
    exp_score = retriever.calculate_experience_match(resume_exp, jd_exp)
    
    print(f"  Experience Match: {exp_score:.2f}")
    
    # Calculate combined score
    combined_score = (
        min(similarity * 12 * 100, 100) * 0.60 +
        skill_score * 0.25 +
        exp_score * 0.15
    )
    
    print(f"  Combined Score: {combined_score:.2f}")
    print(f"\n{'='*50}\n")

def main():
    parser = argparse.ArgumentParser(description='Debug vector similarity between resume and job description')
    parser.add_argument('--resume', required=True, help='Resume ID to test')
    parser.add_argument('--jd', required=True, help='Path to job description file')
    
    args = parser.parse_args()
    
    debug_similarity(args.resume, args.jd)

if __name__ == "__main__":
    main() 