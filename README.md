# Resume Parser & Matching System

A powerful resume parsing and job description matching system using AWS Bedrock embedding models and semantic search.

## Overview

This system helps recruiters and hiring managers efficiently match job descriptions with the most relevant candidate resumes using advanced vector embeddings and natural language understanding.

### Key Features

- **Resume Parsing**: Extract structured data from resumes in various formats (PDF, DOCX, TXT)
- **Semantic Search**: Find candidates based on the meaning of job requirements, not just keywords
- **Multi-factor Ranking**: Rank candidates based on skills, experience, and semantic relevance
- **AWS Integration**: Leverages AWS Bedrock, OpenSearch, S3, and optionally DynamoDB
- **Customizable Matching**: Adjust weights and criteria to find the best candidates for each role

## Getting Started

### Prerequisites

- Python 3.9+
- AWS account with Bedrock and OpenSearch access
- Proper AWS credentials configuration

### Quick Start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment variables:
   ```bash
   cp .env-example .env
   # Edit .env with your AWS credentials and configuration
   ```

3. Parse resumes:
   ```bash
   python parse_resume.py
   ```

4. Match with job description:
   ```bash
   python retrieve_jd_matches.py --jd_file job_descriptions/software_engineer.txt
   ```

For detailed setup and usage instructions, see the [Documentation](./docs/README.md).

## System Architecture

The system uses a multi-tiered architecture:

- **AWS Bedrock**: Generates vector embeddings for semantic search
- **Amazon OpenSearch**: Stores and searches vector embeddings
- **Amazon S3**: Stores raw and processed resume files
- **Optional Databases**:
  - **PostgreSQL**: Stores PII and detailed candidate information
  - **DynamoDB**: Stores structured resume data

## Documentation

Comprehensive documentation is available in the [docs](./docs/README.md) directory:

- [Getting Started Guide](./docs/user-guides/getting-started.md)
- [System Architecture](./docs/architecture/system-architecture.md)
- [Vector Embedding Framework](./docs/technical-docs/vector-embedding-framework.md)
- [Reranking Algorithm](./docs/technical-docs/reranking-algorithm.md)
- [Troubleshooting Guide](./docs/troubleshooting/common-issues.md)

## Example Usage

```python
# Initialize the resume retriever
retriever = ResumeRetriever()

# Search for candidates matching a job description
results = retriever.search_resumes(
    jd_file="job_descriptions/senior_developer.txt",
    method="vector",
    size=20
)

# Process and display results
for candidate in results["matches"]:
    print(f"Candidate: {candidate['name']} - Score: {candidate['score']}")
    print(f"  Matched Skills: {', '.join(candidate['matched_skills'])}")
    print(f"  Experience: {candidate['years_of_experience']} years")
```

## Tools

- **parse_resume.py**: Process and parse resumes from various formats
- **retrieve_jd_matches.py**: Match job descriptions with parsed resumes
- **scripts/create_job_description.py**: Helper to create well-formatted job descriptions

## Command Line Reference

### Resume Parsing

```bash
python parse_resume.py [--file FILE] [--reindex] [--force] [--verbose]
```

### Job Description Matching

```bash
python retrieve_jd_matches.py --jd_file JD_FILE [--method {vector,text,hybrid}] 
                              [--max MAX] [--exp EXP] [--no-rerank]
                              [--weights WEIGHTS] [--output OUTPUT]
```

## License

MIT License

## Acknowledgements

- [AWS Bedrock](https://aws.amazon.com/bedrock/)
- [OpenSearch](https://opensearch.org/)
- [PyMuPDF](https://pymupdf.readthedocs.io/)
- [Spacy](https://spacy.io/)

## Apache Tika Configuration

The resume parser uses Apache Tika for extracting text from DOC files and as a fallback for PDF and DOCX files. Tika requires Java to be installed on your system.

### Prerequisites
- Java 8 or higher installed and available in your PATH
- Python package `tika` (installed via requirements.txt)

### Troubleshooting Tika Issues

If you encounter issues with text extraction from DOC files or see Tika-related errors in the logs, try the following:

1. Check if Java is installed and properly configured:
   ```bash
   python check_java_for_tika.py
   ```

2. Common issues:
   - **Missing Java**: Tika requires Java to function. Install Java 8 or higher.
   - **Tika startup errors**: These can occur if Tika can't start its server process. Check Java installation and connectivity.
   - **Timeout errors**: For large files, try increasing the timeout values in `src/extractors/tika_config.py`.

3. If problems persist:
   - Make sure your Java version is compatible (Java 8 or higher)
   - Try reinstalling the Tika package: `pip install --upgrade tika`
   - Check if your firewall is blocking Tika's server process

### Tika Configuration Settings

The Tika configuration can be modified in `src/extractors/tika_config.py`. Key settings include:

- `TIKA_STARTUP_SLEEP`: Time to wait for Tika server to start
- `TIKA_STARTUP_MAX_RETRY`: Number of retries for server startup
- `TIKA_JAVA_OPTIONS`: JVM options (including memory allocation)

## Resume Parsing Flow

The resume parsing system works in the following order:

1. **Text Extraction** 
   - First, text is extracted from various file formats (PDF, DOCX, DOC, TXT)
   - For PDF files:
     - Primary: PyMuPDF (fitz) is used first
     - Fallback 1: PyPDF is used if PyMuPDF fails
     - Fallback 2: Tika is used only if both PyMuPDF and PyPDF fail
   - For DOCX files: 
     - Primary: docx2txt is used first
     - Fallback 1: python-docx is used if docx2txt fails 
     - Fallback 2: Tika is used only as a last resort
   - For DOC files:
     - Primary: olefile is used first (extracts text directly from OLE file structure)
     - Fallback: Tika is used only if olefile fails to extract sufficient text

2. **Structural Parsing with LLM** (**Primary Parser**)
   - Once text is extracted (regardless of which extraction method was used), the **LLM is always the primary engine** for understanding and structuring the resume content
   - The extracted text is sent to AWS Bedrock LLM with a specialized prompt
   - The LLM analyzes the text and returns structured JSON data
   - This step is the core of the parsing logic and does the heavy lifting
   - Even when Tika is used for text extraction, the actual parsing is still done by the LLM

3. **Post-processing**
   - The structured data from the LLM is normalized and validated
   - Date formats, education details, and experience calculations are standardized
   - The data is prepared for storage in various databases

**Important**: Tika is only a text extraction tool, not a resume parser. It is used only as a fallback when primary extraction methods fail. The actual intelligence and structural understanding of the resume always comes from the LLM, which is the primary parsing engine. 