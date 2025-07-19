# Architectural Separation: Resume Parsing vs. JD Matching

This document explains the architectural separation between the resume parsing functionality and the job description (JD) matching functionality in this system.

## Conceptual Separation

The system is designed with a clear separation of concerns:

1. **Resume Parsing** - A backend service that processes resumes into structured data and stores them in databases.
2. **JD Matching** - A separate service that matches job descriptions against parsed resumes.

This separation allows each component to evolve independently and ensures that the resume parsing functionality is not tied to any specific frontend implementation.

## Implementation Details

### Resume Parsing Component

- **Files**: `parse_resume.py` and modules in `src/`
- **Responsibility**: Extract structured data from resumes and store in databases
- **Production Status**: Fully production-ready

This component:
- Processes resume files (PDF, DOCX, DOC, TXT)
- Uses AWS Bedrock LLMs to extract structured information
- Stores data in PostgreSQL, DynamoDB, and OpenSearch
- Creates embeddings for vector search
- Has no dependencies on JD matching functionality

### JD Matching Component

- **Files**: `retrieve_jd_matches.py`, `debug_similarity.py`
- **Responsibility**: Match job descriptions against parsed resumes
- **Production Status**: For testing and development only

This component:
- Provides a Python-based testing environment for JD matching
- Demonstrates how vector search can be used for resume matching
- Is not intended for production use
- Will be replaced by a React frontend application in production

## Why This Separation?

1. **Different Deployment Patterns**:
   - Resume parsing is a backend service that runs on servers
   - JD matching is primarily a frontend concern that interacts with users

2. **Different Usage Patterns**:
   - Resume parsing is a batch processing operation (process many resumes)
   - JD matching is an interactive operation (users input JDs and view matches)

3. **Different Technology Stacks**:
   - Resume parsing uses Python and AWS services
   - JD matching in production will use React and web technologies

## Production Implementation

In production:

1. The **Resume Parsing** component will continue to be used as-is, running as a backend service to process resumes.

2. The **JD Matching** component will be replaced by a React frontend application that will:
   - Allow users to input job descriptions
   - Query the OpenSearch backend directly for vector matching
   - Display matching resumes in a user-friendly interface
   - Provide filtering, sorting, and other interactive features

## Using Only Resume Parsing

If you're only interested in the resume parsing functionality, you can use:

```bash
python parse_resume.py --file path/to/resume.pdf
```

This will process the resume and store it in the configured databases without any JD matching.

## Testing JD Matching

For testing purposes only, you can use the consolidated JD matching functionality in retrieve_jd_matches.py:

```bash
# Match a single job description file
python retrieve_jd_matches.py --jd_file data/sample_jd.txt

# Process multiple job description files in a directory
python retrieve_jd_matches.py --jd_dir data/

# List available job description files
python retrieve_jd_matches.py --list_jds
```

This will allow you to test how JD matching works, but should not be used in production.

## Database Integration

Both components interact with the same OpenSearch database:

1. **Resume Parsing** creates embeddings and stores them in OpenSearch
2. **JD Matching** (testing) queries OpenSearch to find matching resumes
3. **JD Matching** (production) will query the same OpenSearch backend, but from a React frontend

This shared database architecture allows for seamless integration between the components while maintaining their separation. 