import json
import logging
import hashlib
import time
import boto3
from typing import List, Dict, Any, Optional, Union
from botocore.exceptions import ClientError
import os

# Try to import AWS credentials from aws_credentials.py
try:
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from aws_credentials import (
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_REGION as CREDS_AWS_REGION
    )
    HAS_AWS_CREDENTIALS = True
    logger = logging.getLogger(__name__)
    logger.info(f"Using AWS credentials from aws_credentials.py: {AWS_ACCESS_KEY_ID[:4]}...{AWS_ACCESS_KEY_ID[-4:]}")
except ImportError:
    HAS_AWS_CREDENTIALS = False
    logger = logging.getLogger(__name__)
    logger.warning("AWS credentials not found in aws_credentials.py, using environment variables")

from config.config import BEDROCK_MODEL_ID, BEDROCK_EMBEDDINGS_MODEL, AWS_REGION

logger = logging.getLogger(__name__)

# Global embedding cache
_embedding_cache = {}
_cache_max_size = 500

def create_standardized_text(data: Dict[str, Any]) -> str:
    """
    Create a standardized text representation for embedding generation
    
    Args:
        data: Resume or job description data
        
    Returns:
        Standardized text for embedding generation
    """
    # Extract skills
    skills = data.get('skills', [])
    if isinstance(skills, str):
        skills_text = skills
    elif isinstance(skills, list):
        skills_text = ", ".join(skills)
    else:
        skills_text = ""
    
    # Extract experience
    experience = ""
    if 'total_experience' in data:
        experience = f"{data.get('total_experience')} years"
    elif 'required_experience' in data:
        experience = f"{data.get('required_experience')} years"
    
    # Extract positions
    positions = data.get('positions', [])
    if isinstance(positions, str):
        positions_text = positions
    elif isinstance(positions, list):
        positions_text = ", ".join(positions)
    else:
        positions_text = ""
    
    # Extract job title if it exists (for JDs)
    if 'job_title' in data:
        if positions_text:
            positions_text += f", {data['job_title']}"
        else:
            positions_text = data['job_title']
    
    # Extract education
    education_text = ""
    if 'education' in data and isinstance(data['education'], list):
        education_parts = []
        for edu in data['education']:
            if isinstance(edu, dict):
                degree = edu.get('degree', '')
                institution = edu.get('institution', '')
                if degree and institution:
                    education_parts.append(f"{degree} from {institution}")
                elif degree:
                    education_parts.append(degree)
                elif institution:
                    education_parts.append(institution)
        education_text = ", ".join(education_parts)
    elif 'required_education' in data:
        education_text = data['required_education']
    
    # Extract technologies
    tech_text = ""
    if 'companies' in data and isinstance(data['companies'], list):
        all_techs = []
        for company in data['companies']:
            if isinstance(company, dict) and 'technologies' in company:
                techs = company['technologies']
                if isinstance(techs, list):
                    all_techs.extend(techs)
                elif isinstance(techs, str):
                    all_techs.append(techs)
        tech_text = ", ".join(all_techs)
    
    # Extract summary
    summary = data.get('summary', '')
    if isinstance(summary, dict) and 'text' in summary:
        summary = summary['text']
    
    # Add required skills for JDs
    if 'required_skills' in data and data['required_skills']:
        if skills_text:
            skills_text += ", "
        if isinstance(data['required_skills'], list):
            skills_text += ", ".join(data['required_skills'])
        else:
            skills_text += str(data['required_skills'])
    
    # Add nice-to-have skills for JDs
    if 'nice_to_have_skills' in data and data['nice_to_have_skills']:
        if skills_text:
            skills_text += ", "
        if isinstance(data['nice_to_have_skills'], list):
            skills_text += ", ".join(data['nice_to_have_skills'])
        else:
            skills_text += str(data['nice_to_have_skills'])
    
    # Create standardized text using the common template
    standardized_text = COMMON_TEMPLATE.format(
        skills=skills_text,
        experience=experience,
        positions=positions_text,
        education=education_text,
        technologies=tech_text,
        summary=summary
    )
    
    return standardized_text

def create_standardized_text_for_jd(jd_data: Dict[str, Any]) -> str:
    """
    Create standardized text for job description embedding
    
    Args:
        jd_data: Job description data
        
    Returns:
        Standardized text for embedding generation
    """
    # Use the same function for both resume and JD embeddings
    # to ensure consistent processing
    return create_standardized_text(jd_data)

class BedrockEmbeddings:
    """Class to handle embeddings generation using Amazon Bedrock"""
    
    def __init__(self):
        """Initialize the Bedrock client"""
        # Use explicit credentials if available
        if HAS_AWS_CREDENTIALS:
            logger.info("Using explicit AWS credentials for Bedrock client")
            self.bedrock_runtime = boto3.client(
                service_name="bedrock-runtime",
                region_name=CREDS_AWS_REGION or AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
        else:
            # Fall back to environment variables
            logger.info("Using environment variables for Bedrock client")
            self.bedrock_runtime = boto3.client(
                service_name="bedrock-runtime",
                region_name=AWS_REGION
            )
            
        # Use embedding model from config instead of hardcoding
        self.embedding_model_id = BEDROCK_EMBEDDINGS_MODEL or "amazon.titan-embed-text-v2:0"
        
        # Ensure the model ID includes the version suffix (:0)
        if self.embedding_model_id and not self.embedding_model_id.endswith(":0"):
            self.embedding_model_id += ":0"
            
        logger.info(f"Initialized Bedrock embeddings client with model {self.embedding_model_id}")
    
    def get_embedding(self, text: str, dimension: int = 1024) -> List[float]:
        """
        Get embedding vector for text
        
        Args:
            text: Text to embed
            dimension: Dimension of embedding vector (default 1024)
            
        Returns:
            Embedding vector as list of floats
        """
        logger = logging.getLogger(__name__)
        
        try:
            # For Titan models, always use 1024 dimensions
            if "titan" in self.embedding_model_id.lower():
                dimension = 1024
                logger.info(f"Using fixed dimension 1024 for Titan model")
            # For other models, ensure dimension is valid
            elif dimension not in [256, 512, 1024, 1536]:
                logger.warning(f"Invalid dimension {dimension}, defaulting to 1024")
                dimension = 1024
                
            # Call Bedrock model with appropriate parameters based on model type
            if "titan-embed-text-v1" in self.embedding_model_id.lower():
                # For Titan Embeddings v1 model - using the correct format without embeddingConfig
                request_body = {
                    "inputText": text
                }
            elif "titan-embed-text-v2" in self.embedding_model_id.lower():
                # For Titan Embeddings v2 model - different format
                request_body = {
                    "inputText": text,
                    "dimensions": dimension  # Pass dimension parameter correctly
                }
            elif "cohere" in self.embedding_model_id.lower():
                # For Cohere models
                request_body = {
                    "texts": [text],
                    "input_type": "search_document"
                }
            elif "meta.llama" in self.embedding_model_id.lower():
                # For Meta Llama models
                request_body = {
                    "prompt": text,
                    "max_gen_len": 0,  # Just get the embedding, no generation
                }
            else:
                # Default to Titan v2 parameters as fallback
                request_body = {
                    "inputText": text,
                    "dimensions": dimension
                }
            
            # Log the request body for debugging
            logger.debug(f"Embedding request body: {json.dumps(request_body)[:500]}")
            
            # Invoke Bedrock model
            response = self.bedrock_runtime.invoke_model(
                modelId=self.embedding_model_id,
                body=json.dumps(request_body)
            )
            
            # Parse response based on model type
            response_body = json.loads(response.get('body').read())
            
            if "titan-embed-text-v1" in self.embedding_model_id.lower():
                embedding = response_body.get('embedding')
            elif "titan-embed-text-v2" in self.embedding_model_id.lower():
                embedding = response_body.get('embedding')
            elif "cohere" in self.embedding_model_id.lower():
                embedding = response_body.get('embeddings', [{}])[0].get('values', [])
            elif "meta.llama" in self.embedding_model_id.lower():
                embedding = response_body.get('embedding', [])
            else:
                # Try common response formats
                embedding = (
                    response_body.get('embedding') or
                    response_body.get('vector') or
                    response_body.get('embeddings', [{}])[0].get('values', []) or
                    []
                )
            
            # Validate embedding
            if not embedding or not isinstance(embedding, list):
                logger.error(f"Invalid embedding response: {json.dumps(response_body)[:300]}")
                return [0.0] * dimension
                
            # Log embedding dimension for debugging
            logger.info(f"Generated embedding with dimension {len(embedding)}")
            
            return embedding
            
        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)[:300]}")
            # Return zeros as fallback
            return [0.0] * dimension

# Common template for standardized embedding generation
COMMON_TEMPLATE = """
SKILLS: {skills}
EXPERIENCE: {experience}
POSITIONS: {positions}
EDUCATION: {education}
TECHNOLOGIES: {technologies}
SUMMARY: {summary}
"""

def create_embedded_text(resume_data: Dict) -> str:
    """
    Converts parsed resume data into structured text for embeddings
    Returns: Formatted text string optimized for semantic search
    
    Note: Excludes PII data (name, email, phone, LinkedIn, address)
    """
    sections = []
    
    # Helper function to handle empty/missing data
    def get_value(data, key, default="Not specified"):
        return data.get(key) or default

    # Header Section - Removed full name
    sections.append("# PROFESSIONAL PROFILE")
    
    # Contact Information - Removed completely
    # We don't include any PII data in the embeddings
    
    # Core Summary
    sections.append("\n## PROFESSIONAL SUMMARY")
    sections.append(get_value(resume_data, 'summary', "No summary available"))

    # Experience Overview
    sections.append("\n## EXPERIENCE OVERVIEW")
    sections.append(f"Total Experience: {resume_data.get('total_experience', 0)} years")
    
    if positions := resume_data.get('positions'):
        sections.append(f"Positions Held: {', '.join(positions)}")
    
    # Technical Competencies
    sections.append("\n## TECHNICAL COMPETENCIES")
    if skills := resume_data.get('skills'):
        sections.append("Skills: " + ', '.join(skills))
    if certifications := resume_data.get('certifications'):
        sections.append("Certifications: " + ', '.join(certifications))
    if industries := resume_data.get('industries'):
        sections.append("Industries: " + ', '.join(industries))

    # Professional Experience Details
    sections.append("\n## PROFESSIONAL EXPERIENCE DETAILS")
    for company in resume_data.get('companies', []):
        sections.append(f"\n### {get_value(company, 'name')}")
        sections.append(f"Duration: {get_value(company, 'duration')}")
        sections.append(f"Role: {get_value(company, 'role')}")
        sections.append(f"Technologies: {', '.join(company.get('technologies', []))}")
        sections.append(f"Key Contributions: {get_value(company, 'description')}")

    # Project Highlights
    if projects := resume_data.get('projects'):
        sections.append("\n## KEY PROJECTS")
        for project in projects:
            sections.append(f"\n### {get_value(project, 'name')}")
            sections.append(f"Duration: {project.get('duration_months', 0)} months")
            sections.append(f"Role: {get_value(project, 'role')}")
            sections.append(f"Technologies: {', '.join(project.get('technologies', []))}")
            sections.append(f"Achievement: {get_value(project, 'metrics')}")
            sections.append(f"Description: {get_value(project, 'description')}")

    # Education & Achievements
    sections.append("\n## EDUCATION & ACHIEVEMENTS")
    for edu in resume_data.get('education', []):
        sections.append(
            f"\n- {get_value(edu, 'degree')} "
            f"from {get_value(edu, 'institution')} "
            f"({get_value(edu, 'year', 'Year not specified')})"
        )
    
    if achievements := resume_data.get('achievements'):
        sections.append("\n### Notable Achievements:")
        for achievement in achievements:
            sections.append(
                f"- {get_value(achievement, 'description')} "
                f"[{get_value(achievement, 'type')}] "
                f"({get_value(achievement, 'metrics')})"
            )

    return '\n'.join(sections) 