import os
import logging
import time
import tempfile
import subprocess
from typing import Dict, Any, Optional

# Configure Tika's environment variables
def configure_tika() -> Dict[str, Any]:
    """
    Configure Apache Tika with optimized settings to avoid startup warnings
    
    Returns:
        Dictionary with configuration details
    """
    # Configure Tika logging
    logging.getLogger('tika').setLevel(logging.ERROR)
    
    # Set environment variables for Tika
    os.environ['TIKA_STARTUP_SLEEP'] = '10'  # Increase startup sleep time
    os.environ['TIKA_STARTUP_MAX_RETRY'] = '3'  # Increase retry attempts
    os.environ['TIKA_SERVER_JAR'] = 'https://repo1.maven.org/maven2/org/apache/tika/tika-server/2.9.1/tika-server-2.9.1.jar'
    
    # Java memory settings - increase memory allocation
    os.environ['TIKA_JAVA_OPTIONS'] = '-Xmx1024m'  # Increase memory allocation
    
    # Configure Tika to use the system's temporary directory
    tika_dir = os.path.join(tempfile.gettempdir(), 'tika')
    os.makedirs(tika_dir, exist_ok=True)
    os.environ['TIKA_PATH'] = tika_dir
    
    # Additional settings to help with startup
    os.environ['TIKA_LOG_PATH'] = os.path.join(tika_dir, 'tika.log')
    os.environ['TIKA_STARTUP_PING_TIMEOUT'] = '90'  # Generous timeout
    
    return {
        'tika_dir': tika_dir,
        'tika_startup_sleep': os.environ.get('TIKA_STARTUP_SLEEP'),
        'tika_startup_max_retry': os.environ.get('TIKA_STARTUP_MAX_RETRY'),
        'tika_server_jar': os.environ.get('TIKA_SERVER_JAR'),
        'tika_java_options': os.environ.get('TIKA_JAVA_OPTIONS')
    }

# Initialize Tika server with custom options
def initialize_tika_server() -> Optional[bool]:
    """
    Initialize Tika server with custom options to avoid startup warnings
    
    Returns:
        True if successful, False if failed, None if Tika is not available
    """
    try:
        from tika import parser as tika_parser
        
        # Configure Tika
        config = configure_tika()
        logger = logging.getLogger(__name__)
        logger.info("Initializing Tika server with config: %s", config)
        
        # Check if Java is installed
        try:
            java_version = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT, universal_newlines=True)
            logger.info("Java is installed: %s", java_version.split('\n')[0])
        except Exception as e:
            logger.warning("Java might not be installed or accessible: %s", str(e))
        
        # Start the server explicitly with increased timeout
        try:
            # Try to stop any existing server first
            try:
                from tika import tika
                tika.killServer()
                logger.info("Killed any existing Tika server")
                time.sleep(2)  # Wait for server to fully stop
            except:
                pass
            
            # Create a dummy file for initialization
            with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as temp:
                temp.write(b'test')
                temp_path = temp.name
            
            # Initialize with the dummy file
            logger.info("Initializing Tika server with dummy file: %s", temp_path)
            result = tika_parser.from_file(temp_path, requestOptions={'timeout': 120})
            
            # Clean up
            import os
            try:
                os.unlink(temp_path)
            except:
                pass
            
            if result:
                logger.info("Tika server initialized successfully")
                return True
            else:
                logger.warning("Tika server initialization returned empty result")
                return False
                
        except Exception as e:
            logger.error("Failed to initialize Tika server: %s", str(e))
            return False
    except ImportError:
        return None 