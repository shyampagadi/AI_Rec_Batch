#!/usr/bin/env python3
"""
Script to create necessary PostgreSQL tables for resume parsing
"""

import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.getenv('LOG_FILE', 'create_tables.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def validate_env_vars():
    """Validate required environment variables"""
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your .env file")
        return False
    
    return True

def get_db_connection():
    """Create a connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        logger.info("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        return None

def create_tables():
    """Create necessary tables in PostgreSQL"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            # Create resume_pii table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS "resume_pii" (
                    "resume_id" UUID PRIMARY KEY,
                    "name" TEXT,
                    "email" TEXT,
                    "phone_number" TEXT,
                    "address" TEXT,
                    "linkedin_url" TEXT,
                    "s3_bucket" TEXT,
                    "s3_key" TEXT,
                    "original_filename" TEXT,
                    "file_type" TEXT,
                    "created_dt" TIMESTAMP,
                    "updated_dt" TIMESTAMP
                )
            """)
            
            # Create indexes
            cur.execute('CREATE INDEX IF NOT EXISTS idx_resume_pii_email ON "resume_pii" ("email")')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_resume_pii_name ON "resume_pii" ("name")')
            
            # Add new indexes for duplicate detection
            cur.execute('CREATE INDEX IF NOT EXISTS idx_resume_pii_phone_number ON "resume_pii" ("phone_number")')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_email_phone_combined ON "resume_pii" ("email", "phone_number")')
            
            conn.commit()
            logger.info("Successfully created tables and indexes")
            return True
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        conn.rollback()
        return False
    finally:
        conn.close()

def drop_tables():
    """Drop existing tables"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            # Only drop the resume_pii table
            tables = ["resume_pii"]
            
            for table in tables:
                try:
                    cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                    logger.info(f"Dropped table {table}")
                except Exception as e:
                    logger.error(f"Error dropping table {table}: {str(e)}")
            
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error dropping tables: {str(e)}")
        conn.rollback()
        return False
    finally:
        conn.close()

def main():
    """Main function"""
    try:
        # Validate environment variables
        if not validate_env_vars():
            sys.exit(1)
        
        # Ask if user wants to drop existing tables
        drop_existing = input("Do you want to drop existing tables? (y/n): ").lower() == 'y'
        
        if drop_existing:
            if not drop_tables():
                sys.exit(1)
        
        # Create tables
        if create_tables():
            logger.info("Database setup completed successfully")
            sys.exit(0)
        else:
            logger.error("Database setup failed")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 