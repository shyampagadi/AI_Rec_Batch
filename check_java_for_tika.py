#!/usr/bin/env python3
"""
Script to check if Java is properly installed and configured for Tika
"""
import os
import sys
import logging
import subprocess
import shutil
import platform

# Set up basic logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("tika-java-check")

def check_java_installation():
    """Check if Java is installed and available in PATH"""
    logger.info("Checking Java installation...")
    
    # Check if java command is in PATH
    java_path = shutil.which('java')
    
    if not java_path:
        logger.error("Java is not found in PATH! Tika requires Java to function.")
        logger.info("Please install Java (JRE 8 or higher) and make sure it's in your PATH.")
        
        # Platform-specific guidance
        if platform.system() == 'Windows':
            logger.info("For Windows: Download Java from https://adoptopenjdk.net/ or https://www.oracle.com/java/")
            logger.info("After installation, ensure Java is in your PATH environment variable.")
        elif platform.system() == 'Linux':
            logger.info("For Linux: Install OpenJDK using your package manager:")
            logger.info("  Ubuntu/Debian: sudo apt install default-jre")
            logger.info("  CentOS/RHEL: sudo yum install java-11-openjdk")
        elif platform.system() == 'Darwin':  # macOS
            logger.info("For macOS: Install Java using Homebrew: brew install openjdk")
            
        return False
    
    logger.info(f"Found Java at: {java_path}")
    
    # Check Java version
    try:
        java_version_output = subprocess.check_output(
            ['java', '-version'], 
            stderr=subprocess.STDOUT, 
            universal_newlines=True
        )
        logger.info(f"Java version information: {java_version_output.strip()}")
        
        # Extract version number from output
        import re
        version_match = re.search(r'version "([^"]+)"', java_version_output)
        if version_match:
            version = version_match.group(1)
            logger.info(f"Detected Java version: {version}")
            
            # Check if version is sufficient (needs Java 8 or higher)
            if version.startswith('1.'):  # Java 8 is 1.8.x
                major = int(version.split('.')[1])
                if major < 8:
                    logger.error(f"Java version {version} is too old! Tika requires Java 8 (1.8) or higher.")
                    return False
            
        return True
    except Exception as e:
        logger.error(f"Error checking Java version: {str(e)}")
        return False

def check_tika_installation():
    """Check if Tika is properly installed"""
    logger.info("Checking Tika installation...")
    
    try:
        import tika
        from tika import parser as tika_parser
        
        logger.info(f"Found Tika module, version: {tika.__version__ if hasattr(tika, '__version__') else 'Unknown'}")
        
        # Import configuration module if available
        try:
            from src.extractors.tika_config import configure_tika
            config = configure_tika()
            logger.info(f"Applied Tika configuration: {config}")
        except ImportError:
            logger.warning("Could not import tika_config module, using default configuration")
        
        # Test Tika with a simple text file
        try:
            # Create a simple test file
            test_file = 'tika_test.txt'
            with open(test_file, 'w') as f:
                f.write('This is a test file for Tika.')
            
            logger.info(f"Testing Tika with file: {test_file}")
            result = tika_parser.from_file(test_file, requestOptions={'timeout': 120})
            
            if result and 'content' in result and 'This is a test file' in result['content']:
                logger.info("Tika parsed the test file successfully!")
                success = True
            else:
                logger.warning(f"Tika returned unexpected result: {result.get('content', 'No content')}")
                success = False
                
            # Clean up test file
            os.remove(test_file)
            return success
            
        except Exception as test_error:
            logger.error(f"Error testing Tika: {str(test_error)}")
            return False
            
    except ImportError:
        logger.error("Tika module not found! Install it with: pip install tika")
        return False
    except Exception as e:
        logger.error(f"Error checking Tika: {str(e)}")
        return False

def check_environment_variables():
    """Check if environment variables for Tika are set properly"""
    logger.info("Checking Tika environment variables...")
    
    # List of relevant Tika environment variables
    tika_vars = [
        'TIKA_STARTUP_SLEEP',
        'TIKA_STARTUP_MAX_RETRY',
        'TIKA_SERVER_JAR',
        'TIKA_JAVA_OPTIONS',
        'TIKA_PATH',
        'TIKA_LOG_PATH'
    ]
    
    for var in tika_vars:
        value = os.environ.get(var)
        if value:
            logger.info(f"✓ {var} = {value}")
        else:
            logger.info(f"✗ {var} not set")
    
    return True

def main():
    """Main function to run all checks"""
    print("=" * 60)
    print("Tika and Java Configuration Check")
    print("=" * 60)
    
    java_check = check_java_installation()
    tika_check = check_tika_installation()
    env_check = check_environment_variables()
    
    print("\nSummary:")
    print("-" * 60)
    print(f"Java installation: {'✓ OK' if java_check else '✗ FAILED'}")
    print(f"Tika installation: {'✓ OK' if tika_check else '✗ FAILED'}")
    print(f"Environment variables: {'✓ OK' if env_check else '✗ WARNING'}")
    print("-" * 60)
    
    if not java_check:
        print("\nTika requires Java to function!")
        print("Please install Java 8 or higher and make sure it's in your PATH.")
    
    if java_check and not tika_check:
        print("\nTika is not working properly despite Java being available.")
        print("Try reinstalling Tika: pip install --upgrade tika")
        
    if java_check and tika_check:
        print("\nAll systems go! Tika and Java are properly configured.")

if __name__ == "__main__":
    main() 