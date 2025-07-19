#!/usr/bin/env python
"""
Test script for the OpenSearch Resume Matching API.

This script sends test requests to the API with various job descriptions
and displays the results in a readable format.
"""
import os
import json
import time
import requests
from dotenv import load_dotenv
from pathlib import Path
import urllib.parse
import argparse
import sys
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.markdown import Markdown
import concurrent.futures
from datetime import datetime

# Initialize rich console for better output formatting
console = Console()

# Define output directory
OUTPUT_DIR = Path("C:/Users/MohanS/Downloads/resume_parser_NEW_FINAL_with_docs/resume_parser_NEW_FINAL_with_docs/output/API_output")

# Ensure output directory exists
def ensure_output_dir():
    """Create the output directory if it doesn't exist."""
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        console.print(f"[green]Output directory ready: {OUTPUT_DIR}[/green]")
    except Exception as e:
        console.print(f"[bold red]Error creating output directory: {str(e)}[/bold red]")
        console.print("[yellow]Will not be able to save API responses to files.[/yellow]")

# Save API response to file
def save_response_to_file(response_data, job_description, test_name=None):
    """
    Save API response to a JSON file in the output directory.
    
    Args:
        response_data: The API response data to save
        job_description: The job description used for the query
        test_name: Optional test name to include in the filename
    
    Returns:
        Path to the saved file or None if saving failed
    """
    if not OUTPUT_DIR.exists():
        ensure_output_dir()
    
    try:
        # Create a timestamp for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create a short slug from the job description
        job_slug = job_description.strip().lower()[:30].replace(" ", "_")
        job_slug = ''.join(c for c in job_slug if c.isalnum() or c == '_')
        
        # Create filename with timestamp and job slug
        if test_name:
            filename = f"{timestamp}_{test_name}_{job_slug}.json"
        else:
            filename = f"{timestamp}_{job_slug}.json"
        
        output_path = OUTPUT_DIR / filename
        
        # Save the response data
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(response_data, f, indent=2, ensure_ascii=False)
        
        console.print(f"[green]Response saved to: {output_path}[/green]")
        return output_path
    
    except Exception as e:
        console.print(f"[bold red]Error saving response: {str(e)}[/bold red]")
        return None

# Load environment variables from .env file
def load_environment():
    # Try to find .env file in parent directories if not in current directory
    env_path = Path(".env")
    if not env_path.exists():
        env_path = Path("../.env")
    
    if not env_path.exists():
        console.print("[yellow]Warning: No .env file found. Using environment variables.[/yellow]")
    else:
        load_dotenv(env_path)
    
    # Get API URL from environment
    api_url = os.environ.get("OPENSEARCH_GATEWAY_API_URL")
    if not api_url:
        console.print("[bold red]Error: OPENSEARCH_GATEWAY_API_URL environment variable not set.[/bold red]")
        console.print("[yellow]Please set it in .env file or as an environment variable.[/yellow]")
        sys.exit(1)
    
    # Get API key if available - check both variable names for compatibility
    api_key = os.environ.get("OPENSEARCH_REST_API_KEY")
    if not api_key:
        # Fall back to the original API_KEY variable name if OPENSEARCH_REST_API_KEY is not set
        api_key = os.environ.get("API_KEY")
        if api_key:
            console.print("[yellow]Using API_KEY environment variable. Consider switching to OPENSEARCH_REST_API_KEY for consistency.[/yellow]")
    
    return api_url, api_key

def format_duration(milliseconds):
    """Format milliseconds to a human-readable string."""
    if milliseconds < 1000:
        return f"{milliseconds:.0f}ms"
    elif milliseconds < 60000:
        return f"{milliseconds/1000:.1f}s"
    else:
        return f"{milliseconds/60000:.1f}m"

def test_resume_matching_api(api_url, job_description, api_key=None, analyze_count=None, analyze_all=None, method="get"):
    """
    Test the resume matching API with a job description.
    
    Args:
        api_url: The API URL
        job_description: The job description to search with
        api_key: Optional API key for authentication
        analyze_count: Number of candidates to analyze (optional)
        analyze_all: Whether to analyze all candidates (optional)
        method: HTTP method to use ("get" or "post", default is "get")
    
    Returns:
        API response and time taken
    """
    start_time = time.time()
    console.print(f"[bold blue]Testing API with job description:[/bold blue] {job_description[:80]}...")
    
    # Create parameters
    params = {}
    data = {}
    
    # For POST requests, use request body instead of query parameters
    if method.lower() == "post":
        data = {
            "job_description": job_description
        }
        if analyze_count is not None:
            data["analyze_count"] = analyze_count
        if analyze_all is not None:
            data["analyze_all"] = analyze_all
    else:  # GET request
        params = {"job_description": job_description}
        if analyze_count is not None:
            params["analyze_count"] = str(analyze_count)
        if analyze_all is not None:
            params["analyze_all"] = "true" if analyze_all else "false"
    
    # Set up headers with API key if provided
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    if api_key:
        # Use the standard x-api-key header for API Gateway authentication
        headers["x-api-key"] = api_key
        console.print("[green]Using API key for authentication[/green]")
    else:
        console.print("[yellow]No API key provided - API may require authentication[/yellow]")
    
    # Ensure the URL ends with a trailing slash if needed
    # Some API Gateway configurations are strict about this
    if not api_url.endswith('/'):
        api_url = f"{api_url}/"
    
    # Log the actual request details for debugging
    console.print(f"[dim]Request URL: {api_url}[/dim]")
    console.print(f"[dim]Request Method: {method.upper()}[/dim]")
    console.print(f"[dim]Request Headers: {json.dumps({k: '***' if k.lower() == 'x-api-key' else v for k, v in headers.items()})}[/dim]")
    
    if method.lower() == "post":
        console.print(f"[dim]Request Body: {json.dumps(data)}[/dim]")
    else:
        console.print(f"[dim]Request Params: {json.dumps(params)}[/dim]")
    
    try:
        # Make API request
        if method.lower() == "post":
            response = requests.post(api_url, json=data, headers=headers, timeout=120)
        else:
            response = requests.get(api_url, params=params, headers=headers, timeout=120)
        
        # Calculate time taken
        time_taken = (time.time() - start_time) * 1000  # Convert to ms
        
        # Check response
        if response.status_code == 200:
            try:
                result = response.json()
                console.print(f"[bold green]✓ Success![/bold green] Took {format_duration(time_taken)}")
                return result, time_taken
            except json.JSONDecodeError:
                console.print(f"[bold red]✗ Error: Unable to parse JSON response[/bold red] (Status: {response.status_code})")
                console.print(response.text[:200])
                return None, time_taken
        else:
            console.print(f"[bold red]✗ Error: API returned {response.status_code}[/bold red]")
            console.print(response.text[:200])
            
            # Provide more helpful error messages based on status code
            if response.status_code == 403:
                console.print("[yellow]This might be an authentication issue. Check that your API key is correct.[/yellow]")
                if method.lower() == "get":
                    console.print("[yellow]Try using a POST request instead with --method=post (some APIs require POST for job descriptions).[/yellow]")
            elif response.status_code == 404:
                console.print("[yellow]The API endpoint URL might be incorrect. Check the path.[/yellow]")
            elif response.status_code == 429:
                console.print("[yellow]Rate limit exceeded. Try again later or reduce the frequency of requests.[/yellow]")
            
            return None, time_taken
            
    except requests.RequestException as e:
        time_taken = (time.time() - start_time) * 1000
        console.print(f"[bold red]✗ Request failed: {str(e)}[/bold red]")
        return None, time_taken

def display_result_summary(result, time_taken):
    """Display a summary of the API response."""
    if not result:
        return
    
    console.print("[bold]API Response Summary:[/bold]")
    
    # Create a table for results
    table = Table(show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")
    
    table.add_row("Time Taken", format_duration(time_taken))
    table.add_row("Total Results", str(result.get('total_results', 'N/A')))
    
    job_info = result.get('job_info', {})
    table.add_row("Job Title", job_info.get('title', 'N/A'))
    table.add_row("Required Experience", str(job_info.get('required_experience', 'N/A')))
    
    required_skills = job_info.get('required_skills', [])
    if required_skills:
        table.add_row("Required Skills", ", ".join(required_skills[:5]) + 
                     (f" (+{len(required_skills)-5} more)" if len(required_skills) > 5 else ""))
    
    # Add processing metadata
    metadata = result.get('processing_metadata', {})
    if metadata:
        table.add_row("Processing Time", format_duration(metadata.get('processing_time_ms', 0)))
        table.add_row("Model Used", metadata.get('model_id', 'N/A'))
        table.add_row("Analyzed Candidates", str(metadata.get('analyzed_candidates_count', 'N/A')))
    
    # Add skill gap analysis summary
    skill_gaps = result.get('skill_gap_analysis', [])
    if skill_gaps:
        gap_text = ", ".join([f"{gap['skill']} ({gap['missing_percent']}%)" for gap in skill_gaps[:3]])
        if len(skill_gaps) > 3:
            gap_text += f" (+{len(skill_gaps)-3} more)"
        table.add_row("Top Skill Gaps", gap_text)
    
    console.print(table)

def display_candidate_details(candidates, max_details=3):
    """Display detailed information about top candidates."""
    if not candidates or not isinstance(candidates, list) or len(candidates) == 0:
        console.print("[yellow]No candidate details available[/yellow]")
        return
    
    console.print(f"\n[bold]Top {min(max_details, len(candidates))} Candidate Details:[/bold]")
    
    for i, candidate in enumerate(candidates[:max_details]):
        resume_id = candidate.get('resume_id', 'unknown')
        scores = candidate.get('scores', {})
        
        panel_title = f"Candidate {i+1}: Resume ID: {resume_id}"
        
        # Build candidate content
        content = []
        
        # Add scores section
        content.append("[bold cyan]Scores:[/bold cyan]")
        if scores:
            score_items = [
                f"Overall: {scores.get('overall', 'N/A'):.1f}%",
                f"Skill: {scores.get('skill_match', 'N/A'):.1f}%", 
                f"Experience: {scores.get('experience_match', 'N/A'):.1f}%",
                f"Position: {scores.get('position_match', 'N/A'):.1f}%"
            ]
            content.append(", ".join(score_items))
        
        # Add skills section
        skills_data = candidate.get('skills', {})
        if skills_data:
            content.append("\n[bold cyan]Skills:[/bold cyan]")
            
            # Matching skills
            matching = skills_data.get('matching', [])
            if matching:
                content.append(f"✓ [green]Matching:[/green] {', '.join(matching[:5])}" + 
                             (f" (+{len(matching)-5} more)" if len(matching) > 5 else ""))
            
            # Missing skills
            missing = skills_data.get('missing', [])
            if missing:
                content.append(f"✗ [red]Missing:[/red] {', '.join(missing[:5])}" + 
                             (f" (+{len(missing)-5} more)" if len(missing) > 5 else ""))
        
        # Add experience section
        exp_data = candidate.get('experience', {})
        if exp_data:
            content.append(f"\n[bold cyan]Experience:[/bold cyan] {exp_data.get('years', 'N/A')} years " + 
                         f"({exp_data.get('difference', 0):+.1f} years vs. required)")
        
        # Add professional analysis if available
        analysis = candidate.get('professional_analysis', '')
        if analysis:
            content.append("\n[bold cyan]Analysis:[/bold cyan]")
            content.append(analysis)
        
        # Create panel with all candidate info
        panel = Panel("\n".join(content), title=panel_title, expand=False)
        console.print(panel)

def run_test(api_url, job_description, api_key=None, analyze_count=None, analyze_all=None, test_name=None, method="get"):
    """Run a single API test."""
    console.print(f"\n[bold]===== Testing Resume Matching API =====[/bold]")
    
    result, time_taken = test_resume_matching_api(
        api_url, 
        job_description, 
        api_key=api_key,
        analyze_count=analyze_count, 
        analyze_all=analyze_all,
        method=method
    )
    
    if result:
        display_result_summary(result, time_taken)
        
        # Save response to JSON file
        save_response_to_file(result, job_description, test_name)
        
        # Display top candidates
        candidates = result.get('results', [])
        display_candidate_details(candidates, max_details=3)
    
    return result is not None

def get_test_job_descriptions():
    """Return a list of test job descriptions."""
    return [
        # Test 1: Software Engineer
        "Senior Software Engineer with 5+ years of experience in Python and AWS. "
        "The ideal candidate will have strong expertise in cloud architecture, "
        "serverless computing with Lambda, and experience with databases including "
        "SQL and NoSQL solutions. Knowledge of machine learning frameworks like "
        "TensorFlow or PyTorch is a plus.",
        
        # Test 2: Data Scientist
        "Data Scientist with expertise in machine learning, Python, TensorFlow, and "
        "statistical analysis. Must have at least 3 years of experience working with "
        "large datasets and developing predictive models. Experience with NLP and "
        "deep learning frameworks is required.",
        
        # Test 3: DevOps Engineer
        "DevOps Engineer with strong knowledge of CI/CD pipelines, Docker, Kubernetes, "
        "and infrastructure as code tools like Terraform. At least 4 years of experience "
        "with AWS services including EC2, S3, and CloudFormation. Experience with "
        "monitoring tools and automation is essential."
    ]

def run_batch_tests(api_url, api_key=None, method="get"):
    """Run a batch of tests with different job descriptions."""
    job_descriptions = get_test_job_descriptions()
    
    console.print(f"\n[bold blue]Running batch tests with {len(job_descriptions)} job descriptions[/bold blue]")
    
    success_count = 0
    for i, job_description in enumerate(job_descriptions):
        console.print(f"\n[bold]Test {i+1}/{len(job_descriptions)}[/bold]")
        test_name = f"batch_test_{i+1}"
        if run_test(api_url, job_description, api_key=api_key, test_name=test_name, method=method):
            success_count += 1
    
    console.print(f"\n[bold]{'='*50}[/bold]")
    console.print(f"[bold]Batch Test Results: {success_count}/{len(job_descriptions)} tests passed[/bold]")
    
    if success_count == len(job_descriptions):
        console.print("[bold green]All tests passed! Your API is working correctly.[/bold green]")
    else:
        console.print(f"[bold yellow]{len(job_descriptions) - success_count} tests failed.[/bold yellow]")
        
def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Test the Resume Matching API')
    parser.add_argument('--job', type=str, help='Single job description to test')
    parser.add_argument('--batch', action='store_true', help='Run batch tests with predefined job descriptions')
    parser.add_argument('--analyze-count', type=int, help='Number of candidates to analyze')
    parser.add_argument('--analyze-all', action='store_true', help='Analyze all candidates')
    parser.add_argument('--url', type=str, help='API URL (overrides environment variable)')
    parser.add_argument('--no-save', action='store_true', help='Do not save API responses to files')
    parser.add_argument('--api-key', type=str, help='API key for authentication (overrides environment variable)')
    parser.add_argument('--method', type=str, choices=['get', 'post'], default='get', 
                       help='HTTP method to use (get or post)')
    
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_arguments()
    
    # First, ensure output directory exists if we're saving responses
    if not args.no_save:
        ensure_output_dir()
    
    # Load API URL and API key from environment or command line
    api_url, env_api_key = load_environment()
    api_url = args.url if args.url else api_url
    api_key = args.api_key if args.api_key else env_api_key
    
    console.print(f"[bold]API URL:[/bold] {api_url}")
    
    if args.batch:
        run_batch_tests(api_url, api_key=api_key, method=args.method)
    elif args.job:
        run_test(api_url, args.job, api_key=api_key, analyze_count=args.analyze_count, 
                analyze_all=args.analyze_all, method=args.method)
    else:
        # Default job description if none provided
        default_job = ("Senior Software Engineer with expertise in Python, AWS, and cloud architecture. "
                      "5+ years of experience with backend development and microservices.")
        run_test(api_url, default_job, api_key=api_key, analyze_count=args.analyze_count, 
                analyze_all=args.analyze_all, method=args.method)

if __name__ == "__main__":
    main() 