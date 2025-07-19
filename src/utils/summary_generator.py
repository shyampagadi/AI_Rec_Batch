import os
import time
import colorama
from colorama import Fore, Style, Back
from typing import Dict, List, Any
from datetime import datetime

# Initialize colorama
colorama.init()

class SummaryGenerator:
    """Generate a beautiful summary with metrics at the end of execution"""
    
    def __init__(self):
        """Initialize the summary generator"""
        self.start_time = time.time()
        self.metrics = {}
        self.storage_metrics = {
            "opensearch": {"success": 0, "failed": 0},
            "postgres": {"success": 0, "failed": 0},
            "dynamodb": {"success": 0, "failed": 0},
            "s3": {"success": 0, "failed": 0}
        }
        self.processed_files = []
        self.failed_files = []
        
    def add_processed_file(self, filename: str, resume_id: str, success: bool = True):
        """Add a processed file to the summary"""
        if success:
            self.processed_files.append((filename, resume_id))
        else:
            self.failed_files.append(filename)
    
    def add_storage_result(self, storage_type: str, resume_id: str, success: bool):
        """Add storage result to metrics"""
        if storage_type in self.storage_metrics:
            if success:
                self.storage_metrics[storage_type]["success"] += 1
            else:
                self.storage_metrics[storage_type]["failed"] += 1
    
    def add_metric(self, name: str, value: Any):
        """Add a custom metric to the summary"""
        self.metrics[name] = value
        
    def generate_summary(self) -> str:
        """Generate a beautiful summary string"""
        execution_time = time.time() - self.start_time
        
        # Calculate success rates
        total_files = len(self.processed_files) + len(self.failed_files)
        success_rate = (len(self.processed_files) / total_files * 100) if total_files > 0 else 0
        
        # Build the summary string with colors
        summary = []
        
        # Header
        summary.append(f"\n{Back.BLUE}{Fore.WHITE} RESUME PARSER EXECUTION SUMMARY {Style.RESET_ALL}")
        summary.append(f"\n{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")
        
        # Time information
        summary.append(f"{Fore.YELLOW}Date/Time:{Style.RESET_ALL} {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append(f"{Fore.YELLOW}Execution Time:{Style.RESET_ALL} {execution_time:.2f} seconds")
        
        # Overall metrics
        summary.append(f"\n{Fore.CYAN}OVERALL METRICS{Style.RESET_ALL}")
        summary.append(f"{Fore.YELLOW}Total Files:{Style.RESET_ALL} {total_files}")
        summary.append(f"{Fore.YELLOW}Successfully Processed:{Style.RESET_ALL} {len(self.processed_files)}")
        summary.append(f"{Fore.YELLOW}Failed:{Style.RESET_ALL} {len(self.failed_files)}")
        
        # Success rate with color based on rate
        color = Fore.GREEN if success_rate >= 90 else (Fore.YELLOW if success_rate >= 70 else Fore.RED)
        summary.append(f"{Fore.YELLOW}Success Rate:{Style.RESET_ALL} {color}{success_rate:.1f}%{Style.RESET_ALL}")
        
        # Storage metrics
        summary.append(f"\n{Fore.CYAN}STORAGE METRICS{Style.RESET_ALL}")
        for storage, metrics in self.storage_metrics.items():
            total = metrics["success"] + metrics["failed"]
            if total > 0:
                rate = metrics["success"] / total * 100
                color = Fore.GREEN if rate == 100 else (Fore.YELLOW if rate >= 80 else Fore.RED)
                summary.append(f"{Fore.YELLOW}{storage.capitalize()}:{Style.RESET_ALL} {metrics['success']}/{total} " +
                              f"({color}{rate:.1f}%{Style.RESET_ALL})")
        
        # Custom metrics
        if self.metrics:
            summary.append(f"\n{Fore.CYAN}CUSTOM METRICS{Style.RESET_ALL}")
            for name, value in self.metrics.items():
                summary.append(f"{Fore.YELLOW}{name}:{Style.RESET_ALL} {value}")
        
        # Processed files
        if self.processed_files:
            summary.append(f"\n{Fore.CYAN}SUCCESSFULLY PROCESSED FILES{Style.RESET_ALL}")
            for i, (filename, resume_id) in enumerate(self.processed_files[:5], 1):
                base_filename = os.path.basename(filename)
                summary.append(f"{i}. {Fore.GREEN}{base_filename}{Style.RESET_ALL} -> {Fore.BLUE}{resume_id}{Style.RESET_ALL}")
            
            if len(self.processed_files) > 5:
                summary.append(f"   {Fore.YELLOW}...and {len(self.processed_files) - 5} more{Style.RESET_ALL}")
        
        # Failed files
        if self.failed_files:
            summary.append(f"\n{Fore.CYAN}FAILED FILES{Style.RESET_ALL}")
            for i, filename in enumerate(self.failed_files[:5], 1):
                base_filename = os.path.basename(filename)
                summary.append(f"{i}. {Fore.RED}{base_filename}{Style.RESET_ALL}")
            
            if len(self.failed_files) > 5:
                summary.append(f"   {Fore.YELLOW}...and {len(self.failed_files) - 5} more{Style.RESET_ALL}")
        
        # Footer
        summary.append(f"\n{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")
        
        return "\n".join(summary)
    
    def print_summary(self):
        """Print the summary to console"""
        print(self.generate_summary()) 
        
    def save_summary(self, output_file="output/resume_parsing_summary.txt"):
        """
        Save the summary to a file
        Note: This method is not used in production environments
        """
        # In production environments, we don't write to local files
        # This method is kept for backwards compatibility
        return True 