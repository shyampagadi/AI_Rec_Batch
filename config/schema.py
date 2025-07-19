from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime

@dataclass
class Contact:
    """Contact information from resume"""
    email: Optional[str] = None
    phone: Optional[str] = None
    linkedin: Optional[str] = None
    github: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None

@dataclass
class Education:
    """Education details from resume"""
    institution: str
    degree: str
    field_of_study: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    gpa: Optional[float] = None
    description: Optional[str] = None
    location: Optional[str] = None

@dataclass
class Experience:
    """Work experience details from resume"""
    company: str
    title: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    technologies: List[str] = field(default_factory=list)
    responsibilities: List[str] = field(default_factory=list)
    achievements: List[str] = field(default_factory=list)

@dataclass
class Skill:
    """Skill information from resume"""
    name: str
    category: Optional[str] = None  # e.g., "Programming Languages", "Tools", "Soft Skills"
    level: Optional[str] = None  # e.g., "Expert", "Intermediate", "Beginner"
    years_of_experience: Optional[float] = None

@dataclass
class Project:
    """Project information from resume"""
    name: str
    description: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    technologies: List[str] = field(default_factory=list)
    url: Optional[str] = None
    achievements: List[str] = field(default_factory=list)

@dataclass
class ResumeMetadata:
    """Metadata about the resume document"""
    file_name: str
    file_type: str  # PDF, DOCX, DOC
    file_size: int  # in bytes
    s3_key: str
    extraction_date: datetime = field(default_factory=datetime.now)
    processing_status: str = "pending"  # pending, processed, failed
    processing_error: Optional[str] = None

@dataclass
class Resume:
    """Complete resume data structure"""
    id: str  # Unique identifier
    raw_text: str  # Original extracted text
    metadata: ResumeMetadata
    name: Optional[str] = None
    contact: Contact = field(default_factory=Contact)
    summary: Optional[str] = None
    education: List[Education] = field(default_factory=list)
    experience: List[Experience] = field(default_factory=list)
    skills: List[Skill] = field(default_factory=list)
    projects: List[Project] = field(default_factory=list)
    certifications: List[str] = field(default_factory=list)
    languages: List[str] = field(default_factory=list)
    interests: List[str] = field(default_factory=list)
    additional_sections: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert resume to dictionary for database storage"""
        return {
            "id": self.id,
            "name": self.name,
            "contact": {
                "email": self.contact.email,
                "phone": self.contact.phone,
                "linkedin": self.contact.linkedin,
                "github": self.contact.github,
                "website": self.contact.website,
                "address": self.contact.address
            },
            "summary": self.summary,
            "education": [vars(edu) for edu in self.education],
            "experience": [vars(exp) for exp in self.experience],
            "skills": [vars(skill) for skill in self.skills],
            "projects": [vars(proj) for proj in self.projects],
            "certifications": self.certifications,
            "languages": self.languages,
            "interests": self.interests,
            "additional_sections": self.additional_sections,
            "metadata": {
                "file_name": self.metadata.file_name,
                "file_type": self.metadata.file_type,
                "file_size": self.metadata.file_size,
                "s3_key": self.metadata.s3_key,
                "extraction_date": self.metadata.extraction_date.isoformat(),
                "processing_status": self.metadata.processing_status,
                "processing_error": self.metadata.processing_error
            },
        } 