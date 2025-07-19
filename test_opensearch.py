#!/usr/bin/env python3
"""
Test script for OpenSearch connection
"""

import os
import sys
import logging
from pathlib import Path

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
from dotenv import load_dotenv
env_path = Path(__file__).resolve().parent / '.env'
load_dotenv(dotenv_path=env_path)

# Import OpenSearchHandler
from src.storage.opensearch_handler import OpenSearchHandler

def test_opensearch_connection():
    """Test connection to OpenSearch"""
    try:
        logger.info("Initializing OpenSearch handler...")
        handler = OpenSearchHandler()
        
        logger.info("Testing connection to OpenSearch...")
        # This will initialize the client and test connection
        client = handler.client
        
        logger.info("Ensuring index exists...")
        index_exists = handler.ensure_index_exists()
        logger.info(f"Index exists: {index_exists}")
        
        # Test storing a simple document
        logger.info("Testing document storage...")
        test_data = {
            "name": "Test User",
            "email": "test@example.com",
            "summary": "This is a test document",
            "skills": ["Testing", "Python", "OpenSearch"],
            "total_experience": 5.0
        }
        
        test_text = "This is a test resume text for embedding generation."
        
        import uuid
        test_id = str(uuid.uuid4())
        
        success = handler.store_resume(test_data, test_id, test_text)
        logger.info(f"Document storage success: {success}")
        
        if success:
            logger.info("OpenSearch connection and storage test successful!")
        else:
            logger.error("OpenSearch storage test failed!")
        
        return success
    
    except Exception as e:
        logger.error(f"Error testing OpenSearch connection: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = test_opensearch_connection()
    sys.exit(0 if success else 1) 