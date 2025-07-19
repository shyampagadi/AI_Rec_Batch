import json
import os
import sys
import hashlib
from pathlib import Path
from dotenv import load_dotenv
from lambda_handler import lambda_handler, vector_search, analyze_jd

# Load environment variables from .env file
env_path = Path(__file__).resolve().parent / '.env'
load_dotenv(dotenv_path=env_path)

# Set to True to test without actual AWS services
USE_MOCK_MODE = True

# No need to set environment variables manually as they'll be loaded from .env
# Just check if critical ones are present
required_vars = [
    'OPENSEARCH_ENDPOINT',
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY', 
    'AWS_REGION',
    'BEDROCK_EMBEDDINGS_MODEL',  # Used for embeddings
    'MODEL_ID'                   # Used for LLM-based JD parsing (LLAMA)
]

if not USE_MOCK_MODE:
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file")
        sys.exit(1)

# Print configuration for verification
print("Using configuration:")
print(f"OPENSEARCH_ENDPOINT: {os.environ.get('OPENSEARCH_ENDPOINT')}")
print(f"OPENSEARCH_INDEX: {os.environ.get('OPENSEARCH_INDEX', 'resume-embeddings')}")
print(f"OPENSEARCH_REGION: {os.environ.get('AWS_REGION', 'us-east-1')}")
print(f"OPENSEARCH_SERVERLESS: {os.environ.get('OPENSEARCH_SERVERLESS', 'false')}")
print(f"BEDROCK_EMBEDDINGS_MODEL: {os.environ.get('BEDROCK_EMBEDDINGS_MODEL', 'amazon.titan-embed-text-v2:0')}")
print(f"BEDROCK_MODEL_ID (MODEL_ID): {os.environ.get('MODEL_ID', 'Default model')}")
print(f"MOCK MODE: {USE_MOCK_MODE}")

# Mock implementations for testing without AWS services
def mock_vector_search(jd_text, max_results=10, min_experience=0, enable_reranking=True):
    """Mock implementation of vector search for testing"""
    print(f"\nMOCK: Performing vector search for JD: '{jd_text[:50]}...'")
    print(f"MOCK: Filtering for minimum experience: {min_experience} years")
    print(f"MOCK: Will return {max_results} results")
    print(f"MOCK: Reranking enabled: {enable_reranking}")
    
    # Generate some fake resume IDs
    return [
        f"mock-resume-{i}" for i in range(1, max_results + 1)
    ]

def mock_extract_skills(job_description: str):
    """Mock skill extraction to test functionality"""
    # Extract some skills based on simple keyword detection
    skills = []
    skill_keywords = {
        "python": ["python", "django", "flask", "fastapi", "pandas", "numpy"],
        "java": ["java", "spring", "hibernate", "j2ee", "jvm"],
        "aws": ["aws", "amazon web services", "s3", "ec2", "lambda", "dynamodb"],
        "data": ["sql", "nosql", "database", "postgresql", "mysql", "data lake", "data warehouse"],
        "cloud": ["cloud", "aws", "azure", "gcp", "kubernetes", "docker"]
    }
    
    jd_lower = job_description.lower()
    
    for category, terms in skill_keywords.items():
        for term in terms:
            if term in jd_lower and category not in skills:
                skills.append(category)
                break
    
    return skills

def mock_generate_embedding(text):
    """Mock embedding generation"""
    # Create a simple deterministic hash-based "embedding"
    hash_obj = hashlib.md5(text.encode())
    hash_bytes = hash_obj.digest()
    
    # Create a fake embedding of 1024 dimensions from the hash
    mock_embedding = []
    for i in range(1024):
        # Use the hash to seed a simple value
        value = (hash_bytes[i % 16] / 255.0) * 2 - 1  # Convert to -1 to 1 range
        mock_embedding.append(value)
        
    return mock_embedding

def mock_analyze_jd(jd_text):
    """Mock JD analysis function for testing"""
    # Extract job title
    job_title = "Data Engineer"  # Default title
    if "data scientist" in jd_text.lower():
        job_title = "Data Scientist" 
    elif "software engineer" in jd_text.lower():
        job_title = "Software Engineer"
    elif "machine learning" in jd_text.lower():
        job_title = "Machine Learning Engineer"
        
    # Extract experience
    import re
    exp_pattern = r'(\d+)(?:\+)?\s*(?:years|yrs)'
    exp_matches = re.findall(exp_pattern, jd_text.lower())
    required_experience = int(exp_matches[0]) if exp_matches else 5  # Default to 5 years
    
    # Extract skills
    skills = mock_extract_skills(jd_text)
    
    return {
        "job_title": job_title,
        "required_experience": required_experience,
        "required_skills": skills,
        "nice_to_have_skills": ["communication", "teamwork"],
        "seniority_level": "Senior" if required_experience >= 5 else "Mid-level",
        "job_type": "Full-time",
        "industry": "Technology",
        "required_education": "Bachelor's"
    }

def mock_lambda_handler(event, context):
    """Mock implementation of lambda handler for testing"""
    try:
        print("\nMOCK: Processing API Gateway request")
        
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        jd_text = body.get('job_description', '')
        max_results = int(body.get('max_results', 10))
        enable_reranking = body.get('enable_reranking', True)
        
        print(f"MOCK: Received JD: '{jd_text[:50]}...'")
        print(f"MOCK: Requested {max_results} results")
        print(f"MOCK: Reranking enabled: {enable_reranking}")
        
        # Mock JD analysis
        jd_analysis = mock_analyze_jd(jd_text)
        required_experience = jd_analysis.get('required_experience', 5)
        required_skills = jd_analysis.get('required_skills', [])
        job_title = jd_analysis.get('job_title', 'Not specified')
        
        # Print analysis results
        print(f"MOCK: Analyzed job title: {job_title}")
        print(f"MOCK: Required experience: {required_experience} years")
        print(f"MOCK: Required skills: {', '.join(required_skills)}")
        
        # Mock search
        resume_ids = mock_vector_search(
            jd_text, 
            max_results=max_results, 
            min_experience=required_experience,
            enable_reranking=enable_reranking
        )
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            'body': json.dumps({
                'timestamp': '2025-05-28T10:00:00.000000',
                'total_results': len(resume_ids),
                'resume_ids': resume_ids,
                'job_title': job_title,
                'required_experience': required_experience,
                'required_skills': required_skills,
                'reranking_enabled': enable_reranking
            })
        }
    except Exception as e:
        print(f"MOCK ERROR: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Internal server error: {str(e)}"})
        }

def test_direct_vector_search():
    """Test the vector search function directly"""
    print("\nTesting direct vector search...")
    
    job_description = """
    Senior Data Engineer with 5+ years experience in AWS, Python, and Snowflake.
    Must have experience with data pipelines and ETL processes.
    """
    
    try:
        # Use mock or real function based on mode
        if USE_MOCK_MODE:
            # First analyze the JD
            jd_analysis = mock_analyze_jd(job_description)
            required_experience = jd_analysis.get('required_experience', 5)
            
            print(f"MOCK: Analyzed JD - required experience: {required_experience} years")
            
            # Test both with and without reranking
            print("\nTesting without reranking:")
            resume_ids_no_rerank = mock_vector_search(
                jd_text=job_description,
                max_results=5,
                min_experience=required_experience,
                enable_reranking=False
            )
            
            print("\nTesting with reranking:")
            resume_ids = mock_vector_search(
                jd_text=job_description,
                max_results=5,
                min_experience=required_experience,
                enable_reranking=True
            )
        else:
            # Call the actual search function
            # First analyze the JD
            jd_analysis = analyze_jd(job_description)
            required_experience = jd_analysis.get('required_experience', 0)
            
            print(f"Analyzed JD - required experience: {required_experience} years")
            print(f"Required skills: {', '.join(jd_analysis.get('required_skills', []))}")
            
            # Test both with and without reranking
            print("\nTesting without reranking:")
            resume_ids_no_rerank = vector_search(
                jd_text=job_description,
                max_results=5,
                min_experience=required_experience,
                enable_reranking=False
            )
            
            print("\nTesting with reranking:")
            resume_ids = vector_search(
                jd_text=job_description,
                max_results=5,
                min_experience=required_experience,
                enable_reranking=True
            )
            
            # Compare results
            print("\nComparing results:")
            print(f"Without reranking: {len(resume_ids_no_rerank)} results")
            print(f"With reranking: {len(resume_ids)} results")
            
            # Check for differences
            if set(resume_ids_no_rerank) != set(resume_ids):
                print("The reranking changed the results order or content")
            else:
                print("Reranking did not change the results (expected in mock mode)")
        
        print(f"\nFound {len(resume_ids)} matching resume IDs with reranking:")
        for i, resume_id in enumerate(resume_ids, 1):
            print(f"{i}. {resume_id}")
            
    except Exception as e:
        print(f"Error during vector search: {str(e)}")
        import traceback
        traceback.print_exc()

def test_lambda_handler():
    """Test the full lambda handler"""
    print("\nTesting full lambda handler...")
    
    # Create a mock API Gateway event
    event = {
        "body": json.dumps({
            "job_description": "Data Engineer with 5+ years of Python and AWS experience required. Must be familiar with Snowflake, data warehousing, ETL processes, and data pipeline development. Knowledge of Kubernetes and Docker is a plus.",
            "max_results": 3,
            "enable_reranking": True
        })
    }
    
    # Create a simple mock context object
    class MockContext:
        def __init__(self):
            self.function_name = "local-test"
            self.memory_limit_in_mb = 128
            self.invoked_function_arn = "arn:aws:lambda:local:mock:function"
            self.aws_request_id = "mock-request-id"
    
    try:
        # Use mock or real handler based on mode
        if USE_MOCK_MODE:
            response = mock_lambda_handler(event, MockContext())
        else:
            # Call the actual lambda handler
            response = lambda_handler(event, MockContext())
        
        # Print the formatted response
        print("\nLambda Response:")
        print(json.dumps(response, indent=2))
        
        # If successful, parse and display the resume IDs and other details
        if response.get('statusCode') == 200:
            body = json.loads(response['body'])
            print(f"\nJob Title: {body.get('job_title', 'Not specified')}")
            print(f"Required Experience: {body.get('required_experience', 0)} years")
            print(f"Required Skills: {', '.join(body.get('required_skills', []))}")
            print(f"Reranking Enabled: {body.get('reranking_enabled', False)}")
            print(f"\nFound {body['total_results']} resume IDs:")
            
            for i, resume_id in enumerate(body['resume_ids'], 1):
                print(f"{i}. {resume_id}")
            
            # Now test without reranking
            print("\nTesting without reranking...")
            event_no_rerank = {
                "body": json.dumps({
                    "job_description": "Data Engineer with 5+ years of Python and AWS experience required.",
                    "max_results": 3,
                    "enable_reranking": False
                })
            }
            
            if USE_MOCK_MODE:
                response_no_rerank = mock_lambda_handler(event_no_rerank, MockContext())
            else:
                response_no_rerank = lambda_handler(event_no_rerank, MockContext())
                
            if response_no_rerank.get('statusCode') == 200:
                body_no_rerank = json.loads(response_no_rerank['body'])
                print(f"\nWithout Reranking - Found {body_no_rerank['total_results']} resume IDs:")
                for i, resume_id in enumerate(body_no_rerank['resume_ids'], 1):
                    print(f"{i}. {resume_id}")
        
    except Exception as e:
        print(f"Error during lambda execution: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("Starting local test of Lambda function...")
    
    # Run one or both tests
    test_direct_vector_search()
    test_lambda_handler() 