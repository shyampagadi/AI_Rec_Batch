#!/usr/bin/env python3
import os
import logging
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)

# Import our modules
from src.extractors.text_extractor import TextExtractor

def check_dependencies():
    """Check if key dependencies are available"""
    logger = logging.getLogger(__name__)
    
    # Check for tika
    try:
        import tika
        from tika import parser as tika_parser
        # Initialize tika
        tika_parser.from_file.__init__
        logger.info("Tika is available")
        tika_available = True
    except (ImportError, AttributeError):
        logger.warning("Tika is not available")
        tika_available = False
    
    # Check for olefile
    try:
        import olefile
        logger.info("Olefile is available")
        olefile_available = True
    except ImportError:
        logger.warning("Olefile is not available")
        olefile_available = False
        
    return tika_available, olefile_available

def main():
    """Test DOC file extraction with detailed debugging"""
    logger = logging.getLogger(__name__)
    
    # Check dependencies
    tika_available, olefile_available = check_dependencies()
    
    # Test file path
    doc_file_path = "raw/sample/Mangal Singh Sharma_7 Years_Performance Tester_Hyderabad.doc"
    
    # Ensure the file exists
    if not os.path.exists(doc_file_path):
        logger.error(f"Test file not found: {doc_file_path}")
        return
    
    logger.info(f"Processing .doc file: {doc_file_path}")
    
    try:
        # Try extracting text
        text = TextExtractor.extract_text(doc_file_path, file_type="doc")
        
        # Check if text was extracted
        if text and len(text.strip()) > 0:
            logger.info(f"Successfully extracted {len(text)} characters")
            logger.info(f"First 200 characters: {text[:200]}...")
        else:
            logger.error("Failed to extract any text from the DOC file")
        
        # Try extracting metadata
        logger.info("Attempting to extract metadata...")
        metadata = TextExtractor.extract_metadata(doc_file_path, file_type="doc")
        logger.info(f"Metadata: {metadata}")
        
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main() 