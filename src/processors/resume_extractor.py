import json
import logging
import re
import uuid
import time
import hashlib
from typing import Dict, Any, Optional, Tuple

from config.config import BEDROCK_MAX_INPUT_TOKENS, BEDROCK_CHAR_PER_TOKEN, ENABLE_OPENSEARCH

logger = logging.getLogger(__name__)

# Global cache for resume processing results
_GLOBAL_CACHE = {}
_CACHE_MAX_SIZE = 200

# Global clients
_bedrock_client = None
_opensearch_handler = None

def get_text_hash(text):
    """Generate a stable hash for text content"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

class ResumeExtractor:
    """Extract structured information from resume text using AWS Bedrock"""
    
    def __init__(self, model_id: Optional[str] = None):
        """
        Initialize resume extractor with Bedrock client
        
        Args:
            model_id: AWS Bedrock model ID
        """
        self.model_id = model_id
        # Don't initialize clients here - lazy load when needed
        self._bedrock_client = None
        self._opensearch_handler = None
        logger.info("Initialized ResumeExtractor (clients will be loaded when needed)")
    
    @property
    def bedrock_client(self):
        """Lazy load the Bedrock client"""
        global _bedrock_client
        if _bedrock_client is None:
            from src.utils.bedrock_client import BedrockClient
            _bedrock_client = BedrockClient(model_id=self.model_id)
            logger.info("Initialized Bedrock client")
        return _bedrock_client
    
    @property
    def opensearch_handler(self):
        """Lazy load the OpenSearch handler"""
        global _opensearch_handler
        if ENABLE_OPENSEARCH and _opensearch_handler is None:
            from src.storage.opensearch_handler import OpenSearchHandler
            try:
                _opensearch_handler = OpenSearchHandler()
                logger.info("Initialized OpenSearch handler")
            except Exception as e:
                logger.error(f"Failed to initialize OpenSearch handler: {str(e)}")
                _opensearch_handler = None
        return _opensearch_handler
    
    def process_resume(self, resume_text: str, file_type: str = None, filename: str = None, resume_id: str = None) -> Tuple[Dict[str, Any], str]:
        """
        Process resume text and extract structured information
        
        Args:
            resume_text: Text extracted from resume
            file_type: Type of file (pdf, docx, doc, txt) for context
            filename: Original filename (optional)
            resume_id: Optional existing resume ID to use (ensures consistency across systems)
            
        Returns:
            Tuple of (structured resume data, resume_id)
        """
        logger.info(f"Processing resume text ({len(resume_text)} chars)")
        
        # Use provided ID or generate a new one - this ensures consistency when reprocessing
        if not resume_id:
            resume_id = str(uuid.uuid4())
            logger.info(f"Generated new resume ID: {resume_id}")
        else:
            logger.info(f"Using provided resume ID: {resume_id}")
        
        # Create a prompt for the LLM to extract structured data
        prompt = self._create_extraction_prompt(resume_text, file_type, filename)
        
        # Use Bedrock to extract structured data from resume text
        try:
            # Generate text
            logger.info(f"Sending request to Bedrock {self.model_id}")
            llm_response = self.bedrock_client.generate_text(prompt)
            
            # Parse the response
            resume_data = self._extract_json_from_text(llm_response)
            
            # Apply post-processing to improve extraction quality
            resume_data = self._post_process_extraction(resume_data, resume_text, filename)
            
            # Add resume_id to the data
            resume_data['resume_id'] = resume_id
            
            # Store in OpenSearch if enabled
            if ENABLE_OPENSEARCH and self.opensearch_handler:
                try:
                    logger.info(f"Storing resume data in OpenSearch with ID: {resume_id}")
                    
                    # Compress text for efficient storage
                    compressed_text = self._compress_text(resume_text) if hasattr(self, '_compress_text') else resume_text
                    
                    # Store in OpenSearch
                    success = self.opensearch_handler.store_resume(
                        resume_data=resume_data,
                        resume_id=resume_id,
                        resume_text=compressed_text
                    )
                    
                    if success:
                        logger.info(f"Successfully stored resume in OpenSearch with ID: {resume_id}")
                        resume_data['opensearch_success'] = True
                    else:
                        logger.error(f"Failed to store resume in OpenSearch with ID: {resume_id}")
                        resume_data['opensearch_success'] = False
                        
                except Exception as e:
                    logger.error(f"Error storing resume in OpenSearch: {str(e)}")
                    resume_data['opensearch_success'] = False
            
            return resume_data, resume_id
            
        except Exception as e:
            logger.error(f"Error processing resume: {str(e)}")
            # Return basic data with the resume ID for tracking
            basic_data = {
                "resume_id": resume_id,
                "error": str(e)
            }
            
            # Try to extract name from filename as fallback
            if filename:
                name_match = re.search(r'Naukri_([A-Za-z]+)\[(\d+)y_(\d+)m\]', filename)
                if name_match:
                    basic_data["full_name"] = name_match.group(1)
                    try:
                        years = int(name_match.group(2))
                        months = int(name_match.group(3))
                        basic_data["total_experience"] = years + (months / 12)
                    except:
                        pass
            
            return basic_data, resume_id
    
    def _create_extraction_prompt(self, resume_text: str, file_type: str = None, filename: str = None) -> str:
        """
        Create a prompt for the LLM to extract structured data from resume text
        
        Args:
            resume_text: The resume text
            file_type: The file type (pdf, doc, docx, txt)
            filename: Original filename (optional)
            
        Returns:
            Prompt for the LLM
        """
        # Extract name and experience from filename if present in Naukri format
        name_from_filename = None
        years_exp = None
        months_exp = None
        
        if filename:
            name_match = re.search(r'Naukri_([A-Za-z]+)\[(\d+)y_(\d+)m\]', filename)
            if name_match:
                name_from_filename = name_match.group(1)
                years_exp = name_match.group(2)
                months_exp = name_match.group(3)
                logger.info(f"Extracted from filename: {name_from_filename}, {years_exp}y {months_exp}m")

        # Create a prompt for the LLM
        prompt = """
You are an expert resume parser. Extract information strictly using these rules:

1. Dates: Format as "MM/YYYY-MM/YYYY" (e.g., "01/2022-12/2023")
2. Skills: Normalize to official names (e.g., "LoadRunner" not "Load Runner")
3. Industries: Use official NAICS codes + names (e.g., "5112 - Software Publishers")
4. Certifications: Include issuing body (e.g., "AWS Certified Solutions Architect - Associate (Amazon)")
5. Projects: Extract technologies separately from descriptions
6. Education: Convert years to integer (e.g., 2016 not "2012-2016")

Return JSON with these fields:
{
  "full_name": "",
  "email": "",
  "phone_number": "",
  "linkedin": "",
  "address": "",
  "summary": "(Generate 100+ words if missing)",
  "total_experience": "(Total years of experience, if not provided, calculate from dates)",
  "skills": ["(Standardized names)"],
  "positions": ["(Normalized titles)"],
  "companies": [
    {
      "name": "",
      "duration": "MM/YYYY-MM/YYYY", 
      "description": "",
      "role": "(From position)",
      "technologies": ["(Specific tools used)"]
    }
  ],
  "education": [
    {
      "degree": "",
      "institution": "",
      "year": "(Integer)"
    }
  ],
  "certifications": ["(With issuer)"],
  "achievements": [
    {
      "type": "(Performance/Innovation/Leadership)",
      "description": "",
      "metrics": "(Quantifiable impact)"
    }
  ],
  "industries": ["(NAICS code + name)"],
  "projects": [
  {
    "name": "Name of the project",
    "description": "Description of the project  ",
    "technologies": ["(Specific tools used)"],
    "duration_months": "(Duration in months)",
    "role": "(From position)",
    "metrics": "(Quantifiable impact)"
  }
]
}
"""

        # Add file type specific instructions if needed
        if file_type == 'doc':
            prompt += "\nNOTE: This text was extracted from a .DOC file which may have formatting issues. Extract as much information as possible.\n"
        elif file_type == 'pdf':
            prompt += "\nNOTE: This text was extracted from a PDF file. Look carefully for information that may be in unusual formats.\n"

        # Add name hint if available from filename
        if name_from_filename and years_exp and months_exp:
            prompt += f"\nIMPORTANT: The filename suggests this is {name_from_filename}'s resume with {years_exp}.{months_exp} years of experience.\n"

        # Add resume text and JSON output marker
        prompt += f"""
RESUME TEXT:
{resume_text}

JSON OUTPUT:
"""
        
        return prompt
        
    def _assess_text_quality(self, text: str) -> bool:
        """
        Assess if the extracted text is problematic and needs special handling
        
        Args:
            text: Extracted text from resume
            
        Returns:
            True if text is problematic and needs special handling
        """
        # Check for common indicators of problematic extraction
        issues = 0
        
        # Text is very short
        if len(text) < 500:
            issues += 3
            
        # Text has very few newlines (poor structure)
        if text.count('\n') < 10:
            issues += 2
            
        # Text has abnormal character distribution
        printable_chars = sum(c.isprintable() for c in text)
        if printable_chars / max(len(text), 1) < 0.9:
            issues += 2
            
        # Missing common resume keywords suggests poor extraction
        common_keywords = ['experience', 'education', 'skill', 'project', 'work', 'job', 'resume']
        found_keywords = sum(1 for keyword in common_keywords if keyword.lower() in text.lower())
        if found_keywords < 3:
            issues += 2
            
        # Check for presence of key contact information
        if not re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text):  # No email
            issues += 1
            
        if not re.search(r'[\+\(]?[0-9][0-9\-\(\) ]{8,}[0-9]', text):  # No phone
            issues += 1
            
        logger.debug(f"Text quality assessment: {issues} issues detected")
        
        # Text is deemed problematic if it has more than 3 issues
        return issues > 3
    
    def _post_process_extraction(self, extracted_data: Dict[str, Any], original_text: str, filename: str = None) -> Dict[str, Any]:
        """
        Apply post-processing to improve extraction quality for any resume
        
        Args:
            extracted_data: Data extracted by the LLM
            original_text: Original resume text
            filename: Original filename (optional)
            
        Returns:
            Enhanced extraction data
        """
        if not extracted_data:
            extracted_data = {}
            
        # Ensure result is a dictionary
        if not isinstance(extracted_data, dict):
            logger.warning(f"Extraction result is not a dictionary: {type(extracted_data)}")
            extracted_data = {}
        
        # 1. Extract email if missing
        if 'email' not in extracted_data or not extracted_data['email']:
            email_matches = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', original_text)
            if email_matches:
                extracted_data['email'] = ', '.join(email_matches)
                logger.info(f"Post-processing found email: {extracted_data['email']}")
        
        # 2. Extract phone if missing
        if 'phone_number' not in extracted_data or not extracted_data['phone_number']:
            phone_matches = re.findall(r'[\+\(]?[0-9][0-9\-\(\) ]{8,}[0-9]', original_text)
            if phone_matches:
                extracted_data['phone_number'] = ', '.join(phone_matches)
                logger.info(f"Post-processing found phone: {extracted_data['phone_number']}")
        
        # 3. Extract address if missing 
        if 'address' not in extracted_data or not extracted_data['address']:
            # Try to find address patterns
            address_patterns = [
                # Look for PIN/ZIP code patterns (especially Indian PIN codes)
                r'(?i)(?:address|location|residing at|residence|residing|lives in)[:\s]*([^,\n]*(?:,\s*[^,\n]*){1,3},\s*[^,\n]*\s*[-\s]?\s*\d{5,6})',
                # General address pattern with multiple commas
                r'(?i)(?:address|location|residing at|residence|residing|lives in)[:\s]*([^,\n]*(?:,\s*[^,\n]*){2,5})',
                # Look for PIN/ZIP code elsewhere
                r'(?i)([^,\n]*(?:,\s*[^,\n]*){1,3},\s*[^,\n]*\s*[-\s]?\s*\d{5,6})',
                # City, State PIN pattern (common in Indian resumes)
                r'\b([A-Za-z\s]+,\s*[A-Za-z\s]+\s*[-\s]?\s*\d{5,6})\b'
            ]
            
            # Try each pattern to find an address
            for pattern in address_patterns:
                matches = re.findall(pattern, original_text)
                if matches:
                    for match in matches:
                        if len(match) > 15:  # Avoid very short matches
                            extracted_data['address'] = match.strip()
                            logger.info(f"Post-processing found address: {extracted_data['address']}")
                            break
                    
                    if 'address' in extracted_data:
                        break
            
            # If we still don't have an address, look for lines that might contain addresses
            if 'address' not in extracted_data:
                lines = original_text.split('\n')
                for line in lines:
                    line = line.strip()
                    # Look for lines that might be addresses (contain commas, have PIN/ZIP codes, etc.)
                    if (len(line) > 20 and line.count(',') >= 1 and
                        re.search(r'\d{5,6}', line) and
                        not any(term in line.lower() for term in ['experience', 'job', 'position', 'skill', 'qualification'])):
                        extracted_data['address'] = line
                        logger.info(f"Post-processing found address from line: {extracted_data['address']}")
                        break
        
        # 3. Extract name from filename if name is missing and filename is available
        if ('full_name' not in extracted_data or not extracted_data['full_name']) and filename:
            name_match = re.search(r'Naukri_([A-Za-z]+)\[(\d+)y_(\d+)m\]', filename)
            if name_match:
                extracted_data['full_name'] = name_match.group(1)
                logger.info(f"Post-processing extracted name from filename: {extracted_data['full_name']}")
        
        # 4. Extract experience from filename if experience is missing and filename is available
        if ('total_experience' not in extracted_data or not extracted_data['total_experience']) and filename:
            name_match = re.search(r'Naukri_([A-Za-z]+)\[(\d+)y_(\d+)m\]', filename)
            if name_match:
                try:
                    years = int(name_match.group(2))
                    months = int(name_match.group(3))
                    extracted_data['total_experience'] = years + (months / 12)
                    logger.info(f"Post-processing extracted experience from filename: {extracted_data['total_experience']}")
                except:
                    pass
        
        # 5. Extract skills if missing by looking for skill keywords
        if 'skills' not in extracted_data or not extracted_data['skills']:
            # Common programming languages, frameworks, and technologies
            skill_keywords = [
                # Programming languages
                'python', 'java', 'javascript', 'c\\+\\+', 'c#', 'ruby', 'php', 'swift', 'kotlin',
                # Web technologies
                'html', 'css', 'react', 'angular', 'vue', 'node\\.js', 'django', 'flask',
                # Data science
                'machine learning', 'data science', 'tensorflow', 'pytorch', 'pandas', 'numpy',
                # Databases
                'sql', 'mysql', 'postgresql', 'mongodb', 'oracle', 'dynamodb', 'redis',
                # Cloud
                'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform',
                # Other common skills
                'git', 'agile', 'scrum', 'jira', 'rest api', 'graphql'
            ]
            
            # Find all skills in the text
            found_skills = set()
            for skill in skill_keywords:
                if re.search(r'\b' + skill + r'\b', original_text.lower()):
                    # Convert to title case for better readability
                    found_skills.add(skill.title())
            
            if found_skills:
                extracted_data['skills'] = list(found_skills)
                logger.info(f"Post-processing found {len(found_skills)} skills")
        
        # 6. Initialize empty arrays for common fields if they're missing
        for field in ['companies', 'education', 'certifications']:
            if field not in extracted_data:
                extracted_data[field] = []
        
        # 7. If education is empty but there are education-related terms, try to extract
        if not extracted_data.get('education'):
            education_keywords = [
                r'(?:B\.?Tech|Bachelor of Technology)',
                r'(?:M\.?Tech|Master of Technology)',
                r'(?:B\.?E\.?|Bachelor of Engineering)',
                r'(?:M\.?E\.?|Master of Engineering)',
                r'(?:B\.?Sc\.?|Bachelor of Science)',
                r'(?:M\.?Sc\.?|Master of Science)',
                r'(?:Ph\.?D\.?|Doctor of Philosophy)',
                r'(?:MBA|Master of Business Administration)'
            ]
            
            for keyword in education_keywords:
                matches = re.finditer(keyword, original_text, re.IGNORECASE)
                for match in matches:
                    # Extract a chunk of text around the education keyword
                    start = max(0, match.start() - 100)
                    end = min(len(original_text), match.end() + 150)
                    chunk = original_text[start:end]
                    
                    # Look for a year in this chunk
                    year_match = re.search(r'20[0-2]\d|19[8-9]\d', chunk)
                    year = year_match.group(0) if year_match else None
                    
                    # Look for an institution name
                    inst_match = re.search(r'(?:University|College|Institute|School) of [A-Z][a-z]+|[A-Z][a-z]+ (?:University|College|Institute|School)', chunk)
                    institution = inst_match.group(0) if inst_match else "Unknown Institution"
                    
                    # Add to education
                    education_entry = {
                        "degree": match.group(0),
                        "institution": institution
                    }
                    if year:
                        education_entry["year"] = int(year)
                    
                    extracted_data['education'].append(education_entry)
                    logger.info(f"Post-processing found education: {education_entry}")
        
        return extracted_data
    
    def _preprocess_doc_content(self, text: str) -> str:
        """
        Special preprocessing for text extracted from .doc files to improve LLM parsing
        
        Args:
            text: Raw text from a .doc file
            
        Returns:
            Preprocessed text that's better formatted for LLM consumption
        """
        # Remove special characters and weird formatting from .doc files
        # Replace multiple spaces with a single space
        text = re.sub(r' {2,}', ' ', text)
        
        # Remove non-printable ASCII characters
        text = ''.join(c if c.isascii() and (c.isprintable() or c == '\n') else ' ' for c in text)
        
        # Remove excessive newlines
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Look for common email pattern and ensure it's properly extracted
        email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        if email_match:
            email = email_match.group(0)
            # Ensure the email is properly isolated with spaces
            text = re.sub(r'([^\s])(' + re.escape(email) + r')([^\s])', r'\1 \2 \3', text)
        
        # Look for common phone pattern and ensure it's properly extracted
        phone_patterns = [
            r'\b(?:\+\d{1,3}[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?){1,2}\d{3,4}[-.\s]?\d{3,4}\b',
            r'\b\d{10,12}\b'
        ]
        for pattern in phone_patterns:
            phone_match = re.search(pattern, text)
            if phone_match:
                phone = phone_match.group(0)
                # Ensure the phone number is properly isolated with spaces
                text = re.sub(r'([^\s])(' + re.escape(phone) + r')([^\s])', r'\1 \2 \3', text)
                break
        
        # Identify key sections in the resume
        section_patterns = {
            'contact': r'(email|phone|mobile|tel|contact)',
            'summary': r'(summary|objective|profile|about)',
            'experience': r'(experience|employment|work\s+history|professional)',
            'education': r'(education|academic|qualification)',
            'skills': r'(skills|technical\s+skills|expertise|competencies)',
            'projects': r'(projects|assignments)',
            'certification': r'(certification|accreditation)'
        }
        
        # Try to find potential name at the beginning of the text (usually the first line)
        potential_name = None
        first_lines = text.strip().split('\n')[:3]  # Check first 3 lines
        for line in first_lines:
            line = line.strip()
            # Look for potential name (all words capitalized, no special chars, 2-4 words)
            if (2 <= len(line.split()) <= 4 and 
                all(word[0].isupper() for word in line.split() if word) and
                not re.search(r'[@:/\\(){}[\]]', line)):
                potential_name = line
                break
        
        # Try to structure the text better by identifying key sections
        structured_sections = []
        section_matches = []
        
        # Find all section headers
        for section_name, pattern in section_patterns.items():
            for match in re.finditer(pattern, text, re.IGNORECASE):
                # Get the line containing the potential section header
                line_start = text.rfind('\n', 0, match.start()) + 1
                line_end = text.find('\n', match.start())
                if line_end == -1:
                    line_end = len(text)
                
                # Consider it a section header if it's relatively short
                header_line = text[line_start:line_end].strip()
                if len(header_line) < 50:  # Simple heuristic for header line length
                    section_matches.append((match.start(), section_name, header_line))
        
        # Sort by position
        section_matches.sort()
        
        # If we found enough sections, create a structured format
        if len(section_matches) >= 2:
            # First, add the name and any intro text
            intro_text = ""
            if potential_name:
                intro_text = f"# {potential_name.strip()}\n\n"
            
            if section_matches:
                intro_text += text[:section_matches[0][0]].strip() + "\n\n"
            
            structured_text = intro_text
            
            # Extract sections based on identified headers
            for i, (pos, section_name, header_line) in enumerate(section_matches):
                # Get section content (up until next section or end of text)
                content_start = text.find('\n', pos) + 1
                if content_start <= 0:
                    content_start = pos + len(section_name)
                
                content_end = text.find('\n' + section_matches[i+1][2], pos) if i < len(section_matches) - 1 else len(text)
                
                section_content = text[content_start:content_end].strip()
                
                # Format each section with markdown headers
                structured_sections.append(f"## {header_line}\n{section_content}\n")
            
            # Join all sections
            structured_text += "\n".join(structured_sections)
            
            # If we successfully structured the text, use it
            if len(structured_text.strip()) > len(text.strip()) / 2:  # Ensure we didn't lose content
                logger.info("Rebuilt resume structure from extracted patterns")
                return structured_text
        
        # Try alternative approach if the first one didn't work well
        # Look for capitalized lines that might be section headers
        lines = text.split('\n')
        structured_text = ""
        current_section = ""
        
        if potential_name:
            structured_text = f"# {potential_name.strip()}\n\n"
        
        for line in lines:
            line = line.strip()
            if not line:
                structured_text += "\n"
                continue
                
            # Check if this looks like a section header (capitalized, short, no punctuation except colon)
            if (len(line) < 30 and 
                (line.isupper() or all(word[0].isupper() for word in line.split() if word)) and
                not re.search(r'[.;,!?]', line.replace(':', ''))):
                structured_text += f"\n## {line}\n"
                current_section = line
            else:
                structured_text += line + "\n"
        
        return structured_text
    
    def _compress_text(self, text: str) -> str:
        """Compress text to reduce processing time for embeddings"""
        # Keep only first 5000 chars (enough for semantic understanding)
        if len(text) > 5000:
            return text[:5000]
        return text
    
    def _extract_structured_data(self, resume_text: str, file_type: str = None) -> Dict[str, Any]:
        """
        Extract structured data from resume text using LLM
        
        Args:
            resume_text: Resume text content
            file_type: File type for specialized handling
            
        Returns:
            Structured resume data as dictionary
        """
        # Create prompt for the LLM
        prompt = self._create_extraction_prompt(resume_text, file_type)
        
        # Generate structured data using LLM
        try:
            llm_response = self.bedrock_client.generate_text(
                prompt=prompt,
                max_tokens=2000,  # Reduced from 4000 to speed up processing
                temperature=0.0,  # Use lower temperature for more deterministic output
                top_p=0.9,
                use_cache=True  # Enable response caching
            )
            
            # Parse the JSON output
            return self._parse_llm_response(llm_response, resume_text, file_type)
            
        except Exception as e:
            logger.error(f"Error extracting structured data: {str(e)}")
            return {}
    
    def _extract_doc_structured_data(self, resume_text: str) -> Dict[str, Any]:
        """
        Alternative extraction method specifically for .doc files that had parsing issues
        
        Args:
            resume_text: Resume text content
            
        Returns:
            Structured resume data as dictionary
        """
        # Create a simpler prompt for extracting minimal but essential information
        prompt = """
This is a resume from a .doc file with formatting issues. Extract the following information in JSON format:
- Full name
- Contact information (email, phone, LinkedIn)
- Skills (list of technical skills)
- Work experience (companies and roles)
- Education
- Years of experience

Focus on accuracy over completeness. Return ONLY a valid JSON object with these fields that you're confident about.

Resume text:
"""
        prompt += resume_text
        prompt += """

Return valid JSON in this exact format:
{
  "full_name": "",
  "email": "",
  "phone_number": "",
  "skills": [],
  "total_experience": 0,
  "positions": [],
  "companies": [],
  "education": []
}
"""
        
        try:
            # Generate with simpler prompt
            llm_response = self.bedrock_client.generate_text(
                prompt=prompt,
                max_tokens=1500,
                temperature=0.0,
                top_p=0.9,
                use_cache=True
            )
            
            # Extract JSON with more aggressive parsing
            json_data = self._extract_json_from_text(llm_response) or {}
            
            if not json_data:
                # Try to fix even partial JSON responses
                matches = re.findall(r'{[\s\S]*?}', llm_response)
                for match in matches:
                    try:
                        # Fix common JSON issues
                        fixed_json = match.replace("'", "\"")
                        fixed_json = re.sub(r',\s*}', '}', fixed_json)
                        fixed_json = re.sub(r',\s*]', ']', fixed_json)
                        
                        obj = json.loads(fixed_json)
                        if isinstance(obj, dict):
                            return obj
                    except:
                        continue
                
                # If all else fails, manually extract information using regex
                return self._extract_manual_fallback(resume_text)
            
            return json_data
            
        except Exception as e:
            logger.error(f"Error in alternative extraction method: {str(e)}")
            return self._extract_manual_fallback(resume_text)
    
    def _extract_manual_fallback(self, resume_text: str) -> Dict[str, Any]:
        """
        Last resort method to extract information using regex patterns
        
        Args:
            resume_text: Resume text content
            
        Returns:
            Dictionary with basic extracted information
        """
        logger.info("Using manual extraction fallback to extract resume data")
        
        result = {}
        
        # Extract email
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, resume_text)
        if emails:
            result['email'] = emails[0].lower()
        
        # Extract phone
        phone_pattern = r'\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b'
        phones = re.findall(phone_pattern, resume_text)
        if phones:
            result['phone_number'] = phones[0]
        
        # Try to extract name - look for a name-like pattern at the beginning
        lines = resume_text.split('\n')[:10]  # Check first 10 lines
        for line in lines:
            line = line.strip()
            # Look for potential name (2-3 words with capital letters, no special chars)
            if 3 <= len(line.split()) <= 5 and all(w[0].isupper() for w in line.split() if w):
                if not any(c in line for c in '@:/\\(){}[]'):
                    result['full_name'] = line
                    break
        
        # Extract skills - look for common tech keywords
        tech_keywords = [
            'Java', 'Python', 'JavaScript', 'React', 'SQL', 'NoSQL', 'AWS', 'Azure', 'Docker',
            'Kubernetes', 'Jenkins', 'Git', 'C#', '.NET', 'Node.js', 'Angular', 'Vue', 'PHP',
            'REST', 'API', 'GraphQL', 'Linux', 'Unix', 'HTML', 'CSS', 'DevOps', 'CI/CD',
            'Test', 'QA', 'Performance', 'Load', 'Security', 'Cloud', 'Database', 'Machine Learning'
        ]
        
        found_skills = set()
        for skill in tech_keywords:
            if re.search(r'\b' + re.escape(skill) + r'\b', resume_text, re.IGNORECASE):
                found_skills.add(skill)
        
        if found_skills:
            result['skills'] = list(found_skills)
        else:
            result['skills'] = []
        
        # Try to find years of experience
        exp_patterns = [
            r'(\d+)(?:\+)?\s*(?:years|yrs)(?:\s*of)?\s*(?:experience|exp)',
            r'experience\D*(\d+)(?:\+)?\s*(?:years|yrs)',
            r'(\d+)(?:\+)?\s*(?:years|yrs)'
        ]
        
        for pattern in exp_patterns:
            exp_match = re.search(pattern, resume_text, re.IGNORECASE)
            if exp_match:
                try:
                    result['total_experience'] = float(exp_match.group(1))
                    break
                except ValueError:
                    pass
        
        # Default values for required fields
        if 'full_name' not in result:
            result['full_name'] = "Unknown"
        
        if 'total_experience' not in result:
            result['total_experience'] = 0
        
        # Add empty lists for array fields
        for field in ['positions', 'companies', 'education']:
            if field not in result:
                result[field] = []
        
        return result
    
    def _parse_llm_response(self, llm_response: str, resume_text: str = "", file_type: str = None) -> Dict[str, Any]:
        """
        Parse the LLM response into a structured dictionary
        
        Args:
            llm_response: Raw LLM response text
            resume_text: Original resume text (for fallback extraction)
            file_type: File type of the original document
            
        Returns:
            Structured dictionary
        """
        try:
            # Try to extract JSON from the response
            json_start = llm_response.find('{')
            json_end = llm_response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = llm_response[json_start:json_end]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing JSON from LLM response: {str(e)}")
                    logger.debug(f"Raw response: {llm_response[:500]}...")
                    
                    # Try to fix common JSON issues
                    fixed_json = self._fix_json(json_str)
                    if fixed_json:
                        return fixed_json
                    
                    # For .doc files specifically, try harder to get structured data
                    if file_type == 'doc':
                        logger.info("Using specialized .doc file parsing for LLM response")
                        return self._extract_doc_structured_data(resume_text)
                    
                    # Return fallback response with manually extracted info
                    return self._extract_manual_fallback(resume_text)
            else:
                logger.error("Could not find JSON in LLM response")
                logger.debug(f"Raw response: {llm_response[:500]}...")
                
                # For .doc files, try the alternative extraction
                if file_type == 'doc':
                    logger.info("JSON not found in LLM response, trying specialized .doc extraction")
                    return self._extract_doc_structured_data(resume_text)
                
                # Extract with manual fallback
                return self._extract_manual_fallback(resume_text)
                
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from LLM response: {str(e)}")
            logger.debug(f"Raw response: {llm_response[:500]}...")
            return self._extract_manual_fallback(resume_text)
        except Exception as e:
            logger.error(f"Unexpected error parsing LLM response: {str(e)}")
            return self._extract_manual_fallback(resume_text)
    
    def _fix_json(self, json_str: str) -> Dict[str, Any]:
        """
        Try to fix common JSON formatting issues
        
        Args:
            json_str: Potentially invalid JSON string
            
        Returns:
            Parsed JSON object or empty dict if parsing fails
        """
        try:
            # Try to fix common issues
            # 1. Replace single quotes with double quotes (but not inside strings)
            # 2. Fix trailing commas
            # 3. Add missing quotes to property names
            
            # First try direct parsing
            try:
                return json.loads(json_str)
            except:
                pass
            
            # Replace trailing commas
            json_str = re.sub(r',\s*}', '}', json_str)
            json_str = re.sub(r',\s*]', ']', json_str)
            
            # Try parsing again
            try:
                return json.loads(json_str)
            except:
                pass
            
            # Try with demjson if available (more lenient parser)
            try:
                import demjson3
                return demjson3.decode(json_str)
            except ImportError:
                pass
            except Exception:
                pass
                
            return {}
        except Exception as e:
            logger.error(f"Error fixing JSON: {str(e)}")
            return {}
    
    def _extract_json_from_text(self, text: str) -> Dict[str, Any]:
        """
        Try to extract JSON objects from text that might have markdown or other content
        
        Args:
            text: Text that might contain JSON
            
        Returns:
            Extracted JSON or empty dict
        """
        if not text:
            return {}
        
        # First try to find JSON in code blocks (most common in LLM responses)
        json_matches = re.findall(r'```(?:json)?\s*({[\s\S]*?})\s*```', text)
        if json_matches:
            for json_str in json_matches:
                try:
                    obj = json.loads(json_str)
                    if isinstance(obj, dict) and len(obj) > 3:  # Must have at least 3 keys to be considered valid
                        logger.info(f"Successfully extracted JSON from code block with {len(obj)} fields")
                        return obj
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON from code block: {str(e)}")
                    # Try to fix common issues and try again
                    try:
                        fixed = self._fix_json(json_str)
                        if fixed and len(fixed) > 3:
                            logger.info(f"Successfully fixed JSON from code block with {len(fixed)} fields")
                            return fixed
                    except:
                        pass
        
        # Look for a JSON object that takes up most of the text
        # This assumes the LLM response is primarily the JSON object
        if text.strip().startswith('{') and text.strip().endswith('}'):
            # The whole text appears to be a JSON object
            try:
                cleaned_text = text.strip()
                obj = json.loads(cleaned_text)
                if isinstance(obj, dict) and len(obj) > 3:
                    logger.info(f"Extracted JSON from full text with {len(obj)} fields")
                    return obj
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse full text as JSON: {str(e)}")
                # Try to fix and parse again
                try:
                    fixed = self._fix_json(cleaned_text)
                    if fixed and len(fixed) > 3:
                        logger.info(f"Successfully fixed full text JSON with {len(fixed)} fields")
                        return fixed
                except:
                    pass
        
        # Try to find any JSON-like object with regex
        # Use a more robust pattern that can handle nested objects
        json_pattern = r'\{(?:[^{}]|(?:\{[^{}]*\}))*\}'
        matches = re.findall(json_pattern, text)
        
        valid_candidates = []
        for match in matches:
            try:
                obj = json.loads(match)
                if isinstance(obj, dict):
                    # Score candidates by number of keys
                    valid_candidates.append((obj, len(obj)))
            except:
                # Try to fix and parse
                try:
                    fixed = self._fix_json(match)
                    if fixed:
                        valid_candidates.append((fixed, len(fixed)))
                except:
                    pass
        
        # Sort candidates by score (number of keys) and return the best one
        if valid_candidates:
            valid_candidates.sort(key=lambda x: x[1], reverse=True)
            best_match = valid_candidates[0][0]
            logger.info(f"Found JSON object with {len(best_match)} fields")
            return best_match
        
        # Last resort: manually try to extract key-value pairs from the text
        logger.warning("No valid JSON found, attempting manual key-value extraction")
        try:
            # Look for patterns like "field": "value" or "field": value
            kv_patterns = re.findall(r'"([^"]+)":\s*(?:"([^"]*)"|\[([^\]]*)\]|(\d+))', text)
            if kv_patterns:
                manual_obj = {}
                for match in kv_patterns:
                    key = match[0]
                    # Find which capture group has the value
                    value = next((m for m in match[1:] if m), "")
                    manual_obj[key] = value
                
                if len(manual_obj) > 3:
                    logger.info(f"Manually extracted {len(manual_obj)} fields from text")
                    return manual_obj
        except Exception as e:
            logger.error(f"Manual extraction failed: {str(e)}")
        
        # If all attempts fail, return empty dict
            return {}
    
    def extract_resume_data(self, resume_text: str) -> Dict[str, Any]:
        """
        Extract structured resume data with the specified schema
        
        Args:
            resume_text: Raw text extracted from the resume document
            
        Returns:
            Dictionary with structured resume data
        """
        # Calculate the approximate maximum text length based on model limits
        # Most LLMs have token limits (4k-16k tokens). A token is ~4-6 characters on average.
        # The prompt template also consumes tokens, so we need to leave room for it
        
        # Get the empty prompt template to calculate its approximate token length
        empty_prompt = self._create_extraction_prompt("")
        
        # Estimate token count using configured values
        CHAR_PER_TOKEN = BEDROCK_CHAR_PER_TOKEN  # From config
        TARGET_MAX_TOKENS = BEDROCK_MAX_INPUT_TOKENS  # From config
        
        # Calculate max resume text length
        estimated_prompt_tokens = len(empty_prompt) / CHAR_PER_TOKEN
        max_resume_tokens = TARGET_MAX_TOKENS - estimated_prompt_tokens
        max_resume_chars = int(max_resume_tokens * CHAR_PER_TOKEN)
        
        # Truncate resume text if needed
        original_length = len(resume_text)
        if original_length > max_resume_chars:
            logger.warning(f"Resume text exceeds estimated token limit. Truncating from {original_length} to {max_resume_chars} characters")
            resume_text = resume_text[:max_resume_chars] + "\n\n[Content truncated due to length]"
        
        # Get the prompt from the prompt module
        prompt = self._create_extraction_prompt(resume_text)
        
        try:
            # Invoke Bedrock model
            response = self.bedrock_client.invoke_model(prompt, max_tokens=4000, temperature=0.2)
            
            # Extract and parse JSON from response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {str(e)}")
                    logger.debug(f"Response snippet: {response[:200]}...")
                    
                    # Try to fix common JSON issues
                    logger.info("Attempting to fix JSON formatting issues")
                    fixed_json = self._fix_json(json_str)
                    if fixed_json:
                        logger.info("JSON fixed successfully")
                        return fixed_json
                    
                    # Create a minimal fallback response
                    return self._create_fallback_response(resume_text)
            else:
                logger.error("Could not find JSON object in LLM response")
                logger.debug(f"Response snippet: {response[:200]}...")
                
                # Try to find JSON-like content
                json_obj = self._extract_json_from_text(response)
                if json_obj:
                    return json_obj
                
                # Create a minimal fallback response
                return self._create_fallback_response(resume_text)
                
        except Exception as e:
            logger.error(f"Error extracting resume data: {str(e)}")
            return self._create_fallback_response(resume_text)
    
    def _create_fallback_response(self, resume_text: str) -> Dict[str, Any]:
        """
        Create a minimal fallback response with basic info
        
        Args:
            resume_text: Original resume text
            
        Returns:
            Basic structured response
        """
        logger.warning("Creating fallback response with minimal data")
        
        # Extract basic info using regex patterns
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        phone_pattern = r'\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b'
        
        emails = re.findall(email_pattern, resume_text)
        phones = re.findall(phone_pattern, resume_text)
        
        return {
            "full_name": "Unknown",
            "email": ", ".join(emails).lower() if emails else "",
            "phone_number": ", ".join(phones) if phones else "",
            "address": "Not provided",
            "linkedin": "",
            "summary": "Failed to extract summary from resume.",
            "total_experience": 0,
            "positions": [],
            "companies": [],
            "education": [],
            "skills": [],
            "industries": [],
            "projects": [],
            "achievements": [],
            "certifications": []
        } 