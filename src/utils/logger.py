import os
import logging
import sys
from logging.handlers import RotatingFileHandler
from concurrent_log_handler import ConcurrentRotatingFileHandler
from pathlib import Path
from config.config import LOG_LEVEL, LOG_FILE

def setup_logging(log_level=None, log_dir=None, log_file=None):
    """
    Configure logging for the application
    
    Args:
        log_level: Minimum log level to capture (overrides env var)
        log_dir: Directory to store log files (creates logs/ dir by default)
        log_file: Log file name (overrides env var)
    """
    # Use parameters if provided, otherwise use env vars
    if log_level is None:
        log_level_name = LOG_LEVEL
        log_level = getattr(logging, log_level_name, logging.INFO)
    
    if log_file is None:
        log_file = LOG_FILE
    
    # Create logs directory if it doesn't exist
    if log_dir is None:
        log_dir = Path(__file__).resolve().parent.parent.parent / 'logs'
    
    os.makedirs(log_dir, exist_ok=True)
    
    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicate logs
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)
    
    # Create formatters
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler for general logs
    general_log_file = os.path.join(log_dir, log_file)
    try:
        # Use ConcurrentRotatingFileHandler for multi-process logging
        file_handler = ConcurrentRotatingFileHandler(
            general_log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
    except (ImportError, AttributeError):
        # Fall back to RotatingFileHandler if concurrent handler unavailable
        file_handler = RotatingFileHandler(
            general_log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5
        )
    
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Error log handler
    error_log_file = os.path.join(log_dir, 'errors.log')
    try:
        error_handler = ConcurrentRotatingFileHandler(
            error_log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5
        )
    except (ImportError, AttributeError):
        error_handler = RotatingFileHandler(
            error_log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5
        )
    
    error_handler.setFormatter(formatter)
    error_handler.setLevel(logging.ERROR)
    root_logger.addHandler(error_handler)
    
    # Return logger for convenience
    return root_logger 