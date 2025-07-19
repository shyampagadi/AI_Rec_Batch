#!/usr/bin/env python3
"""
Script to help users create properly formatted job description files
for use with the resume matching system.
"""

import os
import argparse
import datetime
from pathlib import Path

JD_TEMPLATE = """Job Title: {title}

Required Skills:
{skills}

Experience: {experience}+ years

Responsibilities:
{responsibilities}

Qualifications:
{qualifications}

Additional Information:
{additional_info}
"""

def create_job_description(args):
    """Create a job description file based on user input"""
    
    # Get job title
    title = args.title if args.title else input("Enter job title (e.g., Senior Software Engineer): ")
    
    # Get skills
    if args.skills:
        skills = args.skills
    else:
        print("\nEnter required skills, one per line. Enter a blank line when done:")
        skills = []
        while True:
            skill = input("- ")
            if not skill:
                break
            skills.append(skill)
    
    # Format skills
    if isinstance(skills, list):
        skills_formatted = "\n".join(f"- {skill}" for skill in skills)
    else:
        skills_formatted = "\n".join(f"- {skill.strip()}" for skill in skills.split(","))
    
    # Get experience
    experience = args.experience if args.experience else input("\nEnter years of experience required: ")
    
    # Get responsibilities
    if args.responsibilities:
        responsibilities = args.responsibilities
    else:
        print("\nEnter job responsibilities, one per line. Enter a blank line when done:")
        responsibilities = []
        while True:
            resp = input("- ")
            if not resp:
                break
            responsibilities.append(resp)
    
    # Format responsibilities
    if isinstance(responsibilities, list):
        responsibilities_formatted = "\n".join(f"- {resp}" for resp in responsibilities)
    else:
        responsibilities_formatted = "\n".join(f"- {resp.strip()}" for resp in responsibilities.split(","))
    
    # Get qualifications
    if args.qualifications:
        qualifications = args.qualifications
    else:
        print("\nEnter qualifications, one per line. Enter a blank line when done:")
        qualifications = []
        while True:
            qual = input("- ")
            if not qual:
                break
            qualifications.append(qual)
    
    # Format qualifications
    if isinstance(qualifications, list):
        qualifications_formatted = "\n".join(f"- {qual}" for qual in qualifications)
    else:
        qualifications_formatted = "\n".join(f"- {qual.strip()}" for qual in qualifications.split(","))
    
    # Get additional info
    additional_info = args.additional_info if args.additional_info else input("\nEnter any additional information (optional): ")
    
    # Create job description from template
    jd_content = JD_TEMPLATE.format(
        title=title,
        skills=skills_formatted,
        experience=experience,
        responsibilities=responsibilities_formatted,
        qualifications=qualifications_formatted,
        additional_info=additional_info
    )
    
    # Create output directory if it doesn't exist
    output_dir = Path("job_descriptions")
    output_dir.mkdir(exist_ok=True)
    
    # Create filename
    if args.output:
        filename = args.output
    else:
        sanitized_title = "".join(c if c.isalnum() else "_" for c in title.lower())
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        filename = f"{sanitized_title}_{timestamp}.txt"
    
    # Ensure the file has .txt extension
    if not filename.endswith(".txt"):
        filename += ".txt"
    
    # Full output path
    output_path = output_dir / filename
    
    # Write to file
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(jd_content)
    
    print(f"\nJob description created: {output_path}")
    print(f"\nYou can now use this file with the resume matching system:")
    print(f"python retrieve_jd_matches.py --jd_file {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Create a well-formatted job description file")
    parser.add_argument("--title", help="Job title")
    parser.add_argument("--skills", help="Required skills (comma-separated)")
    parser.add_argument("--experience", help="Years of experience required")
    parser.add_argument("--responsibilities", help="Job responsibilities (comma-separated)")
    parser.add_argument("--qualifications", help="Required qualifications (comma-separated)")
    parser.add_argument("--additional-info", help="Additional information")
    parser.add_argument("--output", help="Output filename")
    
    args = parser.parse_args()
    
    create_job_description(args)

if __name__ == "__main__":
    main() 