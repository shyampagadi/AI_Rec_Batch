#!/usr/bin/env python3
import os
import logging
import sys
import json
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)

# Import our modules
from parse_resume import parse_resume_file
from config.config import BEDROCK_MODEL_ID

def main():
    """Test parsing a .doc resume file through the main pipeline"""
    logger = logging.getLogger(__name__)
    
    # Test file path
    doc_file_path = "raw/sample/Mangal Singh Sharma_7 Years_Performance Tester_Hyderabad.doc"
    
    # Ensure the file exists
    if not os.path.exists(doc_file_path):
        logger.error(f"Test file not found: {doc_file_path}")
        return
    
    logger.info(f"Processing .doc resume file: {doc_file_path}")
    
    try:
        # Get the model ID from config
        model_id = BEDROCK_MODEL_ID
        logger.info(f"Using model ID: {model_id}")
        
        # Process the resume
        resume_data, resume_id = parse_resume_file(doc_file_path, file_type="doc", model_id=model_id)
        
        # Check results
        if resume_data:
            logger.info(f"Successfully parsed resume with ID: {resume_id}")
            
            # Save the results to a file for inspection
            output_dir = "output/test_results"
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"parsed_doc_resume_{resume_id}.json")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(resume_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Saved parsed resume to: {output_file}")
            
            # Print some key fields
            name = resume_data.get('full_name', 'N/A')
            skills = resume_data.get('skills', [])
            experience = resume_data.get('total_experience', 'N/A')
            
            logger.info(f"Name: {name}")
            logger.info(f"Total Experience: {experience} years")
            logger.info(f"Number of skills: {len(skills)}")
            logger.info(f"First few skills: {', '.join(skills[:5]) if skills else 'None'}")
        else:
            logger.error("Failed to parse resume - no data returned")
        
    except Exception as e:
        logger.error(f"Error during resume parsing: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main() 