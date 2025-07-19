import os
import logging
import psycopg2
import uuid
import traceback
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import Json
import time

from config.config import (
    ENABLE_POSTGRES,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    get_postgres_connection_string
)

logger = logging.getLogger(__name__)

# Global connection pool
_connection_pool = None

def get_connection_pool(min_conn=1, max_conn=10):
    """
    Get or create the global connection pool
    
    Args:
        min_conn: Minimum number of connections
        max_conn: Maximum number of connections
        
    Returns:
        ThreadedConnectionPool instance
    """
    global _connection_pool
    
    if _connection_pool is None:
        try:
            _connection_pool = ThreadedConnectionPool(
                min_conn,
                max_conn,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info(f"Created PostgreSQL connection pool with {min_conn}-{max_conn} connections")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {str(e)}")
            raise
    
    return _connection_pool

class PostgresHandler:
    """Handle PostgreSQL database operations"""
    
    def __init__(self):
        """Initialize PostgreSQL handler"""
        self.host = POSTGRES_HOST
        self.port = POSTGRES_PORT
        self.database = POSTGRES_DB
        self.user = POSTGRES_USER
        self.password = POSTGRES_PASSWORD
        self.conn = None
        self.pool = None
        self.max_retries = 3  # Add retry count
        self.retry_delay = 1  # Seconds between retries
        self.initialize_pool()
        logger.info(f"PostgreSQL handler initialized for database: {self.database} on {self.host}")
        
    def initialize_pool(self):
        """Initialize connection pool"""
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(
                1,  # Min connections
                10,  # Max connections
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            logger.info(f"Created PostgreSQL connection pool with 1-10 connections")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {str(e)}")
            self.pool = None
            
    def connect(self):
        """Connect to PostgreSQL database with retry logic"""
        for attempt in range(self.max_retries):
            try:
                if self.pool:
                    self.conn = self.pool.getconn()
                else:
                    self.conn = psycopg2.connect(
                        host=self.host,
                        port=self.port,
                        dbname=self.database,
                        user=self.user,
                        password=self.password,
                        connect_timeout=10
                    )
                    
                # Set connection parameters for stability
                if self.conn and not self.conn.closed:
                    self.conn.set_session(autocommit=True)
                    
                logger.info("Connected to PostgreSQL database successfully")
                return True
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1}/{self.max_retries} failed: {str(e)}")
                if self.conn and not self.conn.closed:
                    self.conn.close()
                time.sleep(self.retry_delay)
                
        logger.error(f"Failed to connect to PostgreSQL database after {self.max_retries} attempts")
        return False

    # Add a method to check connection and reconnect if needed
    def ensure_connection(self):
        """Ensure connection is active, reconnect if needed"""
        try:
            if self.conn is None or self.conn.closed:
                logger.info("PostgreSQL connection closed or None, reconnecting...")
                return self.connect()
                
            # Test if connection is still alive with a simple query
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception as e:
            logger.warning(f"Connection test failed: {str(e)}, reconnecting...")
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
            return self.connect()
    
    def close(self):
        """Close connection to PostgreSQL database"""
        try:
            if self.conn:
                if self.pool:
                    self.pool.putconn(self.conn)
                else:
                    self.conn.close()
                logger.info("Closed PostgreSQL connection")
        except Exception as e:
            logger.error(f"Error closing PostgreSQL connection: {str(e)}")
            
    def __enter__(self):
        """Context manager enter"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def ensure_tables_exist(self):
        """Ensure required tables exist in database"""
        try:
            if not self.conn or self.conn.closed:
                self.connect()
            
            with self.conn.cursor() as cursor:
                # First check if the table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'resume_pii'
                    );
                """)
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    # Create resume_pii table if it doesn't exist
                    cursor.execute("""
                        CREATE TABLE resume_pii (
                            resume_id UUID PRIMARY KEY,
                            name TEXT,
                            email TEXT,
                            phone_number TEXT,
                            address TEXT,
                            linkedin_url TEXT,
                            s3_bucket TEXT,
                            s3_key TEXT,
                            original_filename TEXT,
                            file_type TEXT,
                            created_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                        CREATE INDEX IF NOT EXISTS idx_resume_pii_email ON resume_pii (email);
                        CREATE INDEX IF NOT EXISTS idx_resume_pii_phone_number ON resume_pii (phone_number);
                    """)
                    logger.info("Created resume_pii table")
                
                # Now create the tables for educational background and work experience if they don't exist
                # Educational Background
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS resume_education (
                        education_id SERIAL PRIMARY KEY,
                        resume_id UUID REFERENCES resume_pii(resume_id) ON DELETE CASCADE,
                        degree TEXT,
                        institution TEXT,
                        year INTEGER,
                        created_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Work Experience
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS resume_experience (
                        experience_id SERIAL PRIMARY KEY,
                        resume_id UUID REFERENCES resume_pii(resume_id) ON DELETE CASCADE,
                        company_name TEXT,
                        role TEXT,
                        duration TEXT,
                        description TEXT,
                        created_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Skills
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS resume_skills (
                        skill_id SERIAL PRIMARY KEY,
                        resume_id UUID REFERENCES resume_pii(resume_id) ON DELETE CASCADE,
                        skill_name TEXT,
                        created_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Commit changes
                self.conn.commit()
                logger.info("Successfully created tables")
                
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            traceback.print_exc()
            if self.conn:
                self.conn.rollback()
            raise
            
    def find_resume_by_contact_info(self, email=None, phone_number=None):
        """
        Find a resume by email or phone number
        
        Args:
            email: Email address to search for
            phone_number: Phone number to search for
            
        Returns:
            Tuple containing (resume_id, email, phone_number) or None if not found
        """
        if not email and not phone_number:
            return None
            
        try:
            if not self.conn or self.conn.closed:
                self.connect()
                
            query = "SELECT resume_id, email, phone_number FROM resume_pii WHERE "
            params = []
            
            if email:
                # Normalize email (lowercase)
                email = email.lower().strip()
                # Use LOWER() for case-insensitive matching
                query += "LOWER(email) = LOWER(%s)"
                params.append(email)
                
            if phone_number:
                # Normalize phone (remove all non-digits)
                normalized_phone = ''.join(filter(str.isdigit, phone_number))
                
                if email:
                    query += " OR "
                
                # Use regular expression to match phone numbers regardless of formatting
                # This will match any phone that has the same digits in the same order
                query += "REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g') = %s"
                params.append(normalized_phone)
                
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                
                if result:
                    logger.info(f"Found existing resume with ID: {result[0]}")
                    return result
                    
                return None
        except Exception as e:
            logger.error(f"Error looking up resume by contact info: {str(e)}")
            return None
            
    def update_resume_pii(self, resume_id, resume_data, s3_bucket=None, s3_key=None, original_filename=None, file_type=None):
        """
        Update an existing resume in PostgreSQL
        
        Args:
            resume_id: UUID of the resume to update
            resume_data: Updated resume data dictionary
            s3_bucket: S3 bucket name (optional)
            s3_key: S3 object key (optional)
            original_filename: Original filename (optional)
            file_type: File type (optional)
            
        Returns:
            resume_id if successful, None otherwise
        """
        try:
            if not self.conn or self.conn.closed:
                self.connect()
                
            with self.conn.cursor() as cursor:
                # Update main resume PII table
                update_query = """
                UPDATE resume_pii 
                SET name = %s, 
                    email = %s, 
                    phone_number = %s, 
                    address = %s, 
                    linkedin_url = %s,
                    updated_dt = CURRENT_TIMESTAMP
                """
                
                params = [
                    resume_data.get('name', resume_data.get('full_name')),
                    resume_data.get('email'),
                    resume_data.get('phone_number'),
                    resume_data.get('address'),
                    resume_data.get('linkedin_url', resume_data.get('linkedin'))
                ]
                
                # Add S3 fields if provided
                if s3_bucket or s3_key or original_filename or file_type:
                    if s3_bucket:
                        update_query += ", s3_bucket = %s"
                        params.append(s3_bucket)
                        
                    if s3_key:
                        update_query += ", s3_key = %s"
                        params.append(s3_key)
                        
                    if original_filename:
                        update_query += ", original_filename = %s"
                        params.append(original_filename)
                        
                    if file_type:
                        update_query += ", file_type = %s"
                        params.append(file_type)
                
                # Add WHERE clause
                update_query += " WHERE resume_id = %s"
                params.append(resume_id)
                
                # Execute update
                cursor.execute(update_query, params)
                
                # Check if update was successful
                if cursor.rowcount == 0:
                    logger.warning(f"No rows updated for resume ID: {resume_id}")
                    return None
                    
                # Update education if present
                if 'education' in resume_data and isinstance(resume_data['education'], list):
                    # Delete existing education records
                    cursor.execute("DELETE FROM resume_education WHERE resume_id = %s", [resume_id])
                    
                    # Insert new education records
                    for edu in resume_data['education']:
                        if not isinstance(edu, dict):
                            continue
                            
                        cursor.execute(
                            "INSERT INTO resume_education (resume_id, degree, institution, year) VALUES (%s, %s, %s, %s)",
                            [
                                resume_id,
                                edu.get('degree', ''),
                                edu.get('institution', ''),
                                edu.get('year', None)
                            ]
                        )
                
                # Update experience/companies if present
                if 'companies' in resume_data and isinstance(resume_data['companies'], list):
                    # Delete existing experience records
                    cursor.execute("DELETE FROM resume_experience WHERE resume_id = %s", [resume_id])
                    
                    # Insert new experience records
                    for company in resume_data['companies']:
                        if not isinstance(company, dict):
                            continue
                            
                        cursor.execute(
                            "INSERT INTO resume_experience (resume_id, company_name, role, duration, description) VALUES (%s, %s, %s, %s, %s)",
                            [
                                resume_id,
                                company.get('name', ''),
                                company.get('role', ''),
                                company.get('duration', ''),
                                company.get('description', '')
                            ]
                        )
                
                # Update skills if present
                if 'skills' in resume_data and isinstance(resume_data['skills'], list):
                    # Delete existing skills
                    cursor.execute("DELETE FROM resume_skills WHERE resume_id = %s", [resume_id])
                    
                    # Insert new skills
                    for skill in resume_data['skills']:
                        cursor.execute(
                            "INSERT INTO resume_skills (resume_id, skill_name) VALUES (%s, %s)",
                            [resume_id, skill]
                        )
                
                # Commit the transaction
                self.conn.commit()
                logger.info(f"Successfully updated resume with ID: {resume_id}")
                return resume_id
                
        except Exception as e:
            logger.error(f"Error updating resume in PostgreSQL: {str(e)}")
            if self.conn:
                self.conn.rollback()
            return None
    
    def insert_resume_pii(
        self,
        resume_data: Dict[str, Any],
        s3_bucket: str,
        s3_key: str,
        original_filename: str,
        file_type: str,
        resume_id: Optional[str] = None
    ) -> str:
        """
        Insert or update resume PII data
        
        Args:
            resume_data: Resume data dictionary
            s3_bucket: S3 bucket name
            s3_key: S3 object key
            original_filename: Original filename
            file_type: File type (pdf, docx, etc.)
            resume_id: Optional resume ID (UUID)
            
        Returns:
            Resume ID (UUID)
        """
        conn = self.connect()
        
        # First check if this resume already exists by email or phone number
        email = resume_data.get('email')
        phone_number = resume_data.get('phone_number')
        
        if email or phone_number:
            existing = self.find_resume_by_contact_info(email, phone_number)
            if existing:
                # Use the existing resume ID instead
                existing_id = existing[0]
                logger.info(f"Found existing resume by contact info with ID: {existing_id}")
                resume_id = existing_id
        
        # Generate UUID if not provided and not found by contact info
        if resume_id is None:
            resume_id = str(uuid.uuid4())
        
        try:
            with conn.cursor() as cursor:
                # Insert or update resume PII data
                cursor.execute("""
                    INSERT INTO resume_pii (
                        resume_id, name, email, phone_number, address, linkedin_url,
                        s3_bucket, s3_key, original_filename, file_type, created_dt, updated_dt
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (resume_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        email = EXCLUDED.email,
                        phone_number = EXCLUDED.phone_number,
                        address = EXCLUDED.address,
                        linkedin_url = EXCLUDED.linkedin_url,
                        s3_bucket = EXCLUDED.s3_bucket,
                        s3_key = EXCLUDED.s3_key,
                        original_filename = EXCLUDED.original_filename,
                        file_type = EXCLUDED.file_type,
                        updated_dt = CURRENT_TIMESTAMP
                    RETURNING resume_id
                """, (
                    resume_id,
                    resume_data.get('name', ''),
                    resume_data.get('email', ''),
                    resume_data.get('phone_number', ''),
                    resume_data.get('address', ''),
                    resume_data.get('linkedin_url', ''),
                    s3_bucket,
                    s3_key,
                    original_filename,
                    file_type
                ))
                
                result = cursor.fetchone()
                conn.commit()
                
                logger.info(f"Inserted/updated resume PII data with ID: {resume_id}")
                return str(result[0]) if result else resume_id
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting resume PII data: {str(e)}")
            raise
    
    def batch_insert_resume_pii(self, batch_data):
        """
        Batch insert or update resume PII data
        
        Args:
            batch_data: List of dictionaries with resume data
            
        Returns:
            List of resume IDs
        """
        if not batch_data:
            return []
            
        # Ensure connection is active
        if not self.ensure_connection():
            logger.error("Failed to establish PostgreSQL connection for batch insert")
            raise Exception("Failed to establish PostgreSQL connection")
            
        resume_ids = []
        cursor = None
        
        try:
            # Set autocommit to False for batch transaction
            self.conn.autocommit = False
            cursor = self.conn.cursor()
            
            for item in batch_data:
                resume_data = item['resume_data']
                s3_bucket = item['s3_bucket']
                s3_key = item['s3_key']
                original_filename = item.get('original_filename', '')
                file_type = item.get('file_type', '')
                resume_id = item.get('resume_id')
                
                if not resume_id:
                    resume_id = str(uuid.uuid4())
                
                # Extract fields from resume_data
                name = resume_data.get('name', resume_data.get('full_name', ''))
                email = resume_data.get('email', '')
                phone = resume_data.get('phone_number', '')
                linkedin = resume_data.get('linkedin_url', resume_data.get('linkedin', ''))
                address = resume_data.get('address', '')
                
                # Prepare data for upsert
                try:
                    cursor.execute("""
                        INSERT INTO resume_pii (
                            resume_id, name, email, phone_number, linkedin_url, address,
                            s3_bucket, s3_key, original_filename, file_type, created_dt, updated_dt
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (resume_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            email = EXCLUDED.email,
                            phone_number = EXCLUDED.phone_number,
                            linkedin_url = EXCLUDED.linkedin_url,
                            address = EXCLUDED.address,
                            s3_bucket = EXCLUDED.s3_bucket,
                            s3_key = EXCLUDED.s3_key,
                            original_filename = EXCLUDED.original_filename,
                            file_type = EXCLUDED.file_type,
                            updated_dt = NOW()
                    """, (
                        resume_id, name, email, phone, linkedin, address,
                        s3_bucket, s3_key, original_filename, file_type
                    ))
                    
                    resume_ids.append(resume_id)
                    
                except Exception as e:
                    logger.error(f"Error inserting/updating resume {resume_id}: {str(e)}")
                    # Attempt to reconnect and retry once
                    if "connection" in str(e).lower():
                        if self.ensure_connection():
                            cursor = self.conn.cursor()
                            try:
                                cursor.execute("""
                                    INSERT INTO resume_pii (
                                        resume_id, name, email, phone_number, linkedin_url, address,
                                        s3_bucket, s3_key, original_filename, file_type, created_dt, updated_dt
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                                    ON CONFLICT (resume_id) DO UPDATE SET
                                        name = EXCLUDED.name,
                                        email = EXCLUDED.email,
                                        phone_number = EXCLUDED.phone_number,
                                        linkedin_url = EXCLUDED.linkedin_url,
                                        address = EXCLUDED.address,
                                        s3_bucket = EXCLUDED.s3_bucket,
                                        s3_key = EXCLUDED.s3_key,
                                        original_filename = EXCLUDED.original_filename,
                                        file_type = EXCLUDED.file_type,
                                        updated_dt = NOW()
                                """, (
                                    resume_id, name, email, phone, linkedin, address,
                                    s3_bucket, s3_key, original_filename, file_type
                                ))
                                resume_ids.append(resume_id)
                            except Exception as retry_error:
                                logger.error(f"Retry failed for resume {resume_id}: {str(retry_error)}")
            
            # Commit the transaction after all inserts
            self.conn.commit()
            logger.info(f"Batch inserted/updated {len(resume_ids)} resume PII records")
            
        except Exception as e:
            # Roll back the transaction if there was an error
            if self.conn and not self.conn.closed:
                self.conn.rollback()
                
            logger.error(f"Error in batch insert: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Try to reconnect and restart the whole batch if it's a connection issue
            if "connection" in str(e).lower() and self.ensure_connection():
                return self.batch_insert_resume_pii(batch_data)
            
        finally:
            # Reset autocommit to true
            if self.conn and not self.conn.closed:
                self.conn.autocommit = True
                
            # Close cursor
            if cursor:
                cursor.close()
                
        return resume_ids
    
    def get_resume_pii(self, resume_id: str) -> Optional[Dict[str, Any]]:
        """
        Get resume PII data by ID
        
        Args:
            resume_id: Resume ID (UUID)
            
        Returns:
            Resume PII data dictionary or None if not found
        """
        if not ENABLE_POSTGRES:
            return None
            
        try:
            if not self.conn or self.conn.closed:
                self.connect()
                
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = "SELECT * FROM resume_pii WHERE resume_id = %s;"
                cursor.execute(query, (resume_id,))
                result = cursor.fetchone()
                
                return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"Error getting resume PII data: {str(e)}")
            return None 