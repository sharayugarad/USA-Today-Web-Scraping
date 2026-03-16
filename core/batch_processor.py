#!/usr/bin/env python3
"""
Batch Processor for Mass ARB Scraper System
Handles batch processing of URLs with progress tracking, duplicate prevention,
and last-run time tracking for date filtering.
"""

import json
import os
import time
import hashlib
import glob
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional
from pathlib import Path

from config.settings import BATCH_CONFIG, SCRAPED_DIR, PROGRESS_DIR, SCRAPER_REGISTRY
from core.notifier import EmailNotifier


class BatchProcessor:
    """
    Handles batch processing of URLs with progress tracking and duplicate prevention.
    Now includes last-run time tracking for date filtering.
    """

    def __init__(self, batch_size=None, delay_minutes=None, progress_file=None, last_run_file=None):
        """
        Initialize BatchProcessor.
        
        Args:
            batch_size (int): Number of URLs per batch (default from config)
            delay_minutes (int): Minutes to wait between batches (default from config)
            progress_file (str): Path to progress file (default from config)
            last_run_file (str): Path to last run tracking file (default from config)
        """
        self.batch_size = batch_size or BATCH_CONFIG['batch_size']
        self.delay_seconds = (delay_minutes or BATCH_CONFIG['delay_minutes']) * 60
        self.progress_file = progress_file or BATCH_CONFIG['progress_file']
        self.last_run_file = last_run_file or BATCH_CONFIG['last_run_file']
        self.notifier = EmailNotifier()
        
        # Data storage
        self.existing_urls = set()
        self.sent_urls = set()
        self.url_hashes = set()
        self.last_run_times = {}  # NEW: Track last run time per scraper
        
        # Load existing data
        self.load_existing_data()
        self.load_last_run_times()  # NEW: Load last run times
        
        print(f"[BATCH PROCESSOR] Initialized with batch_size={self.batch_size}, delay={self.delay_seconds//60}min")

    def load_existing_data(self):
        """Load all existing URLs from JSON files and progress files."""
        try:
            # Load from all scraped data files
            data_files = list(SCRAPED_DIR.glob("*.json"))
            
            for file_path in data_files:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Handle different JSON structures
                    if isinstance(data, list):
                        for item in data:
                            self._add_url_to_cache(item)
                    elif isinstance(data, dict):
                        # Check for 'data' key or other common structures
                        if 'data' in data:
                            for item in data['data']:
                                self._add_url_to_cache(item)
                        elif 'urls' in data:
                            for item in data['urls']:
                                self._add_url_to_cache(item)
                        else:
                            # Treat dict itself as an item with URL
                            self._add_url_to_cache(data)
                            
                except json.JSONDecodeError as e:
                    print(f"[WARNING] Could not parse {file_path.name}: {e}")
                except Exception as e:
                    print(f"[WARNING] Error loading {file_path.name}: {e}")
            
            # Load from progress file
            if self.progress_file.exists():
                try:
                    with open(self.progress_file, 'r', encoding='utf-8') as f:
                        progress = json.load(f)
                        sent_urls = progress.get('sent_urls', [])
                        for url in sent_urls:
                            self.sent_urls.add(url)
                            url_hash = hashlib.md5(url.encode()).hexdigest()
                            self.url_hashes.add(url_hash)
                except Exception as e:
                    print(f"[WARNING] Could not load progress file: {e}")
            
            print(f"[BATCH PROCESSOR] Loaded {len(self.existing_urls)} existing URLs")
            print(f"[BATCH PROCESSOR] Loaded {len(self.sent_urls)} previously sent URLs")
            print(f"[BATCH PROCESSOR] Loaded {len(self.url_hashes)} URL hashes")
            
        except Exception as e:
            print(f"[ERROR] Failed to load existing data: {e}")

    def load_last_run_times(self):
        """Load last run times for each scraper (NEW)."""
        try:
            if self.last_run_file.exists():
                with open(self.last_run_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.last_run_times = data
                    
                print(f"[BATCH PROCESSOR] Loaded last run times for {len(self.last_run_times)} scrapers")
                
                # Show last run times
                for scraper_key, info in self.last_run_times.items():
                    last_run = info.get('last_run', 'Never')
                    print(f"  - {scraper_key}: {last_run}")
            else:
                print(f"[BATCH PROCESSOR] No previous run times found (first run)")
                
        except Exception as e:
            print(f"[WARNING] Could not load last run times: {e}")
            self.last_run_times = {}

    def save_last_run_times(self):
        """Save last run times for each scraper (NEW)."""
        try:
            with open(self.last_run_file, 'w', encoding='utf-8') as f:
                json.dump(self.last_run_times, f, indent=2, ensure_ascii=False)
            
            print(f"[BATCH PROCESSOR] Last run times saved to {self.last_run_file.name}")
            
        except Exception as e:
            print(f"[ERROR] Could not save last run times: {e}")

    def get_last_run_time(self, scraper_key: str) -> Optional[datetime]:
        """
        Get last run time for a specific scraper (NEW).
        
        Args:
            scraper_key (str): Scraper key (e.g., 'clarkson')
            
        Returns:
            datetime: Last run time or None if never run
        """
        if scraper_key not in self.last_run_times:
            return None
        
        last_run_str = self.last_run_times[scraper_key].get('last_run')
        if not last_run_str:
            return None
        
        try:
            # Parse ISO format datetime
            last_run = datetime.fromisoformat(last_run_str.replace('Z', '+00:00'))
            return last_run
        except Exception as e:
            print(f"[WARNING] Could not parse last run time for {scraper_key}: {e}")
            return None

    def update_last_run_time(self, scraper_key: str, scraper_name: str, urls_found: int, success: bool = True):
        """
        Update last run time for a scraper (NEW).
        
        Args:
            scraper_key (str): Scraper key (e.g., 'clarkson')
            scraper_name (str): Human-readable scraper name
            urls_found (int): Number of URLs found
            success (bool): Whether scraping was successful
        """
        now = datetime.now(timezone.utc)
        
        self.last_run_times[scraper_key] = {
            'scraper_name': scraper_name,
            'last_run': now.isoformat(),
            'last_success': success,
            'urls_found': urls_found,
            'timestamp': now.isoformat()
        }
        
        # Save immediately
        self.save_last_run_times()
        
        print(f"[BATCH PROCESSOR] Updated last run time for {scraper_name}: {now.isoformat()}")

    def _add_url_to_cache(self, item):
        """
        Add URL from item to cache.
        
        Args:
            item: Can be a dict with 'url' key or a string URL
        """
        url = None
        
        if isinstance(item, dict):
            # Try different common key names
            url = item.get('url') or item.get('link') or item.get('href')
        elif isinstance(item, str):
            url = item
        
        if url:
            self.existing_urls.add(url)
            url_hash = hashlib.md5(url.encode()).hexdigest()
            self.url_hashes.add(url_hash)

    def is_duplicate(self, url: str) -> bool:
        """
        Check if URL is a duplicate using multiple methods.
        
        Args:
            url (str): URL to check
            
        Returns:
            bool: True if duplicate, False otherwise
        """
        # Direct URL comparison
        if url in self.existing_urls:
            return True
        
        # Hash comparison for better duplicate detection
        url_hash = hashlib.md5(url.encode()).hexdigest()
        if url_hash in self.url_hashes:
            return True
        
        return False

    def is_already_sent(self, url: str) -> bool:
        """
        Check if URL was already sent.
        
        Args:
            url (str): URL to check
            
        Returns:
            bool: True if already sent, False otherwise
        """
        return url in self.sent_urls

    def filter_unique_urls(self, new_links: List[Dict]) -> List[Dict]:
        """
        Filter out duplicate and already-sent URLs with enhanced detection.
        
        Args:
            new_links (List[Dict]): List of link dictionaries
            
        Returns:
            List[Dict]: Filtered unique links
        """
        unique_links = []
        duplicates_found = 0
        already_sent_found = 0
        
        for link in new_links:
            # Extract URL from different possible structures
            url = None
            if isinstance(link, dict):
                url = link.get('url') or link.get('link') or link.get('href')
            elif isinstance(link, str):
                url = link
                link = {'url': url, 'title': ''}  # Convert to dict
            
            if not url:
                continue
            
            # Check if it's a duplicate
            if self.is_duplicate(url):
                duplicates_found += 1
                continue
            
            # Check if it was already sent
            if self.is_already_sent(url):
                already_sent_found += 1
                continue
            
            # It's a new unique URL
            unique_links.append(link)
            self.existing_urls.add(url)
            url_hash = hashlib.md5(url.encode()).hexdigest()
            self.url_hashes.add(url_hash)
        
        if duplicates_found > 0:
            print(f"[BATCH PROCESSOR] Filtered out {duplicates_found} duplicate URLs")
        if already_sent_found > 0:
            print(f"[BATCH PROCESSOR] Filtered out {already_sent_found} already-sent URLs")
        
        return unique_links

    def get_custom_subject(self, scraper_name):
        """
        Get custom subject format for the scraper.
        
        Args:
            scraper_name (str): Name of the scraper
            
        Returns:
            str: Custom email subject
        """
        # Search for scraper in registry
        for category, scrapers in SCRAPER_REGISTRY.items():
            for key, info in scrapers.items():
                if info['name'] == scraper_name:
                    return info.get('email_subject', f"{scraper_name} Scrapped Link")
        
        # Fallback
        return f"{scraper_name} Scrapped Link"

    def save_progress(self):
        """Save current progress to file."""
        try:
            progress = {
                'last_updated': datetime.now().isoformat(),
                'sent_urls': list(self.sent_urls),
                'total_sent': len(self.sent_urls),
                'batch_size': self.batch_size,
                'delay_minutes': self.delay_seconds // 60
            }
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress, f, indent=2, ensure_ascii=False)
            
            print(f"[BATCH PROCESSOR] Progress saved to {self.progress_file.name}")
            
        except Exception as e:
            print(f"[ERROR] Could not save progress: {e}")

    def extract_urls_from_links(self, links: List) -> List[str]:
        """
        Extract URL strings from various link formats.
        
        Args:
            links (List): List of links (can be dicts, strings, or mixed)
            
        Returns:
            List[str]: List of URL strings
        """
        urls = []
        for link in links:
            if isinstance(link, dict):
                url = link.get('url') or link.get('link') or link.get('href')
                if url:
                    urls.append(url)
            elif isinstance(link, str):
                urls.append(link)
        return urls

    def process_batch(self, all_urls: List, scraper_name: str, scraper_key: str = None) -> bool:
        """
        Process URLs in batches with email notifications.
        
        Args:
            all_urls (List): List of URL dictionaries or strings
            scraper_name (str): Name of the scraper for email subject
            scraper_key (str): Scraper key for last run tracking (NEW)
            
        Returns:
            bool: True if processing completed successfully
        """
        print(f"\n{'='*60}")
        print(f"BATCH PROCESSING: {scraper_name}")
        print(f"{'='*60}")
        
        # Handle empty input
        if not all_urls:
            print(f"[BATCH PROCESSOR] No URLs provided for {scraper_name}")
            
            # Update last run time even if no URLs found (NEW)
            if scraper_key:
                self.update_last_run_time(scraper_key, scraper_name, 0, True)
            
            return True
        
        # Filter out duplicates and already-sent URLs
        unique_links = self.filter_unique_urls(all_urls)
        
        if not unique_links:
            print(f"[BATCH PROCESSOR] No new unique URLs found for {scraper_name}")
            
            # Update last run time (NEW)
            if scraper_key:
                self.update_last_run_time(scraper_key, scraper_name, 0, True)
            
            return True
        
        print(f"[BATCH PROCESSOR] Total NEW UNIQUE URLs to process: {len(unique_links)}")
        print(f"[BATCH PROCESSOR] Batch size: {self.batch_size}, Delay: {self.delay_seconds//60} minutes")
        
        # Calculate total batches
        total_batches = (len(unique_links) + self.batch_size - 1) // self.batch_size
        batch_number = 1
        
        print(f"[BATCH PROCESSOR] Will send {total_batches} batches")
        
        # Get the custom subject ONCE (consistent for all batches of this scraper)
        custom_subject = self.get_custom_subject(scraper_name)
        print(f"[BATCH PROCESSOR] Email subject: {custom_subject}")
        
        # Process each batch
        for i in range(0, len(unique_links), self.batch_size):
            batch = unique_links[i:i + self.batch_size]
            batch_urls = self.extract_urls_from_links(batch)
            
            print(f"\n[BATCH {batch_number}/{total_batches}] Processing {len(batch_urls)} URLs...")
            print(f"[BATCH {batch_number}] Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Create batch info (for email body, not subject)
            batch_info = {
                'batch_number': batch_number,
                'total_batches': total_batches,
                'urls_in_batch': len(batch_urls)
            }
            
            # Send email for this batch with CLEAN subject (no batch info in subject line)
            success = self.notifier.send_notification(
                new_urls=batch_urls,
                subject_prefix=custom_subject,
                scraper_name=scraper_name,
                batch_info=batch_info,
                use_custom_subject_only=True
            )
            
            if success:
                # Mark URLs as sent
                for url in batch_urls:
                    self.sent_urls.add(url)
                
                # Save progress
                self.save_progress()
                
                print(f"[BATCH {batch_number}] Successfully sent {len(batch_urls)} URLs")
                
                # Wait before next batch (except for the last batch)
                if batch_number < total_batches:
                    print(f"[BATCH {batch_number}] Waiting {self.delay_seconds//60} minutes before next batch...")
                    time.sleep(self.delay_seconds)
            else:
                print(f"[BATCH {batch_number}] Failed to send email. Stopping batch process.")
                
                # Update last run time with failure status (NEW)
                if scraper_key:
                    self.update_last_run_time(scraper_key, scraper_name, len(unique_links), False)
                
                return False
            
            batch_number += 1
        
        print(f"\n[SUCCESS] Batch processing completed for {scraper_name}")
        print(f"[SUCCESS] Total URLs processed: {len(unique_links)}")
        print(f"[SUCCESS] Total batches sent: {total_batches}")
        print(f"{'='*60}\n")
        
        # Update last run time with success (NEW)
        if scraper_key:
            self.update_last_run_time(scraper_key, scraper_name, len(unique_links), True)
        
        return True

    def get_stats(self) -> Dict:
        """
        Get processing statistics.
        
        Returns:
            Dict: Statistics dictionary
        """
        return {
            'total_existing_urls': len(self.existing_urls),
            'total_sent_urls': len(self.sent_urls),
            'total_url_hashes': len(self.url_hashes),
            'batch_size': self.batch_size,
            'delay_minutes': self.delay_seconds // 60,
            'total_scrapers_tracked': len(self.last_run_times),
            'last_updated': datetime.now().isoformat()
        }

    def reset_progress(self):
        """Reset all progress (use with caution)."""
        self.sent_urls.clear()
        self.save_progress()
        print("[BATCH PROCESSOR] Progress reset - all URLs will be sent again")

    def reset_last_run_times(self):
        """Reset all last run times (use with caution) (NEW)."""
        self.last_run_times.clear()
        self.save_last_run_times()
        print("[BATCH PROCESSOR] Last run times reset - all scrapers will process full history")

    def print_stats(self):
        """Print current statistics."""
        stats = self.get_stats()
        print("\n" + "="*60)
        print("BATCH PROCESSOR STATISTICS")
        print("="*60)
        print(f"Total Existing URLs: {stats['total_existing_urls']}")
        print(f"Total Sent URLs: {stats['total_sent_urls']}")
        print(f"Total URL Hashes: {stats['total_url_hashes']}")
        print(f"Batch Size: {stats['batch_size']}")
        print(f"Delay: {stats['delay_minutes']} minutes")
        print(f"Scrapers Tracked: {stats['total_scrapers_tracked']}")
        print(f"Last Updated: {stats['last_updated']}")
        
        # Show last run times (NEW)
        if self.last_run_times:
            print("\n" + "-"*60)
            print("LAST RUN TIMES:")
            print("-"*60)
            for scraper_key, info in self.last_run_times.items():
                last_run = info.get('last_run', 'Never')
                urls_found = info.get('urls_found', 0)
                status = "✓" if info.get('last_success', True) else "✗"
                print(f"  {status} {scraper_key}: {last_run} ({urls_found} URLs)")
        
        print("="*60 + "\n")


def test_batch_processor():
    """Test the batch processor with sample data."""
    print("\n" + "="*60)
    print("TESTING BATCH PROCESSOR")
    print("="*60)
    
    # Create sample data
    sample_urls = []
    for i in range(45):  # Create 45 URLs to test batching
        sample_urls.append({
            'url': f'https://example.com/test-url-{i+1}',
            'title': f'Test URL {i+1}',
            'description': f'This is test URL number {i+1}'
        })
    
    # Initialize processor with small batches for testing
    processor = BatchProcessor(batch_size=10, delay_minutes=0.1)
    
    print(f"Sample data: {len(sample_urls)} URLs")
    print(f"Batch size: {processor.batch_size}")
    print(f"Expected batches: {(len(sample_urls) + processor.batch_size - 1) // processor.batch_size}")
    
    # Process the batch
    success = processor.process_batch(sample_urls, "Test Scraper", "test_scraper")
    
    if success:
        print("\nSUCCESS: Batch processor test PASSED!")
        processor.print_stats()
    else:
        print("\nERROR: Batch processor test FAILED")
    
    print("="*60 + "\n")
    return success


if __name__ == "__main__":
    test_batch_processor()