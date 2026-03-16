#!/usr/bin/env python3
"""
Scraper Orchestrator - WITH SINGLE DAILY DIGEST
Coordinates execution of all scrapers and sends ONE consolidated email.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

from config.settings import SCRAPER_REGISTRY
from core.batch_processor import BatchProcessor

import importlib
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

from config.settings import SCRAPER_REGISTRY, get_scraper_info, get_all_enabled_scrapers
from core.batch_processor import BatchProcessor


class ScraperOrchestrator:
    """
    Orchestrates the execution of multiple scrapers.
    Sends a single consolidated daily digest email.
    """
    
    # def __init__(self, digest_mode: bool = True):
    #     """
    #     Initialize the orchestrator.
        
    #     Args:
    #         digest_mode (bool): If True, collect all URLs and send one email.
    #                            If False, send separate emails per scraper.
    #     """
    #     self.logger = logging.getLogger(__name__)
    #     self.batch_processor = BatchProcessor()
    #     self.digest_mode = digest_mode
        
    #     # Execution tracking
    #     self.results = {}
    #     self.start_time = None
    #     self.end_time = None
        
    #     # Digest collection (all URLs from all scrapers)
    #     self.digest_urls = []  # All URLs collected
    #     self.digest_by_scraper = {}  # Organized by scraper
        
    #     self.logger.info(f"Scraper orchestrator initialized (Digest Mode: {digest_mode})")
    
    def __init__(self, digest_mode: bool = True, scraper_registry: dict = None, email_subject: str = None):
        """
        Initialize the orchestrator.
        
        Args:
            digest_mode (bool): If True, collect all URLs and send one email.
                            If False, send separate emails per scraper.
            scraper_registry (dict): Custom scraper registry to use (optional).
                                    If None, uses the default SCRAPER_REGISTRY.
            email_subject (str): Custom email subject line (optional).
                            If None, uses "Daily Mass Arbitration Links".
        """
        self.logger = logging.getLogger(__name__)
        self.batch_processor = BatchProcessor()
        self.digest_mode = digest_mode
        
        # Allow custom scraper registry (for running different sets of scrapers)
        self.scraper_registry = scraper_registry if scraper_registry is not None else SCRAPER_REGISTRY
        
        # Allow custom email subject (for different email types)
        self.custom_email_subject = email_subject
        
        # Execution tracking
        self.results = {}
        self.start_time = None
        self.end_time = None
        
        # Digest collection (all URLs from all scrapers)
        self.digest_urls = []  # All URLs collected
        self.digest_by_scraper = {}  # Organized by scraper
        
        self.logger.info(f"Scraper orchestrator initialized (Digest Mode: {digest_mode})")
        if email_subject:
            self.logger.info(f"Custom email subject: {email_subject}")
    
    def _import_scraper(self, module_path: str, class_name: str):
        """
        Dynamically import a scraper class.
        
        Args:
            module_path (str): Module path (e.g., 'scrapers.law_firms.clarkson')
            class_name (str): Class name (e.g., 'ClarksonScraper')
            
        Returns:
            Scraper class or None if import fails
        """
        try:
            from importlib import import_module
            module = import_module(module_path)
            scraper_class = getattr(module, class_name)
            return scraper_class
        except Exception as e:
            self.logger.error(f"Failed to import {module_path}.{class_name}: {e}")
            return None
    
    def run_specific_scraper(self, scraper_key: str) -> Optional[Dict]:
        """
        Run a specific scraper by its key.
        
        Args:
            scraper_key (str): Scraper key (e.g., 'clarkson')
            
        Returns:
            dict: Scraper results or None if not found
        """
        # Find scraper in registry
        scraper_info = None
        for category, scrapers in self.scraper_registry.items():
            if scraper_key in scrapers:
                scraper_info = scrapers[scraper_key]
                break
        
        if not scraper_info:
            self.logger.error(f"Scraper '{scraper_key}' not found in registry")
            return None
        
        # Check if enabled
        if not scraper_info.get('enabled', True):
            self.logger.info(f"Scraper '{scraper_key}' is disabled, skipping")
            return None
        
        return self._execute_scraper(scraper_key, scraper_info)
    
    def _execute_scraper(self, scraper_key: str, scraper_info: Dict) -> Dict:
        """
        Execute a single scraper.
        
        Args:
            scraper_key (str): Scraper key
            scraper_info (dict): Scraper configuration
            
        Returns:
            dict: Execution results
        """
        scraper_name = scraper_info['name']
        module_path = scraper_info['module']
        class_name = scraper_info['class']
        
        self.logger.info(f"Starting scraper: {scraper_name}")
        
        result = {
            'scraper_key': scraper_key,
            'scraper_name': scraper_name,
            'success': False,
            'urls_found': 0,
            'urls_data': [],
            'error': None,
            'start_time': datetime.now(),
            'end_time': None,
            'duration': 0
        }
        
        try:
            # Import scraper class
            scraper_class = self._import_scraper(module_path, class_name)
            if not scraper_class:
                result['error'] = "Failed to import scraper class"
                return result
            
            # Instantiate and run scraper
            scraper = scraper_class()
            scraped_data = scraper.run()
            
            result['urls_found'] = len(scraped_data)
            result['urls_data'] = scraped_data
            result['success'] = True
            
            # In digest mode, collect URLs instead of sending immediately
            # if self.digest_mode and scraped_data:
            #     self.logger.info(f"Collecting {len(scraped_data)} URLs for digest")
            #     self.digest_urls.extend(scraped_data)
            #     self.digest_by_scraper[scraper_name] = scraped_data
            
            # In digest mode, collect URLs instead of sending immediately
            if self.digest_mode and scraped_data:
                self.logger.info(f"Collecting {len(scraped_data)} URLs for digest")
                
                # Filter out duplicates using batch processor
                unique_urls = []
                for url_data in scraped_data:
                    url = url_data.get('url') if isinstance(url_data, dict) else url_data
                    
                    # Check if this URL was already sent before
                    if not self.batch_processor.is_duplicate(url):
                        unique_urls.append(url_data)
                        # Mark as sent immediately
                        self.batch_processor._add_url_to_cache(url_data)
                    else:
                        self.logger.debug(f"Skipping duplicate URL: {url}")
                
                if unique_urls:
                    self.digest_urls.extend(unique_urls)
                    self.digest_by_scraper[scraper_name] = unique_urls
                    self.logger.info(f"Added {len(unique_urls)} unique URLs to digest (filtered out {len(scraped_data) - len(unique_urls)} duplicates)")
                else:
                    self.logger.info(f"All {len(scraped_data)} URLs were duplicates - skipped")
            
            # In non-digest mode, process immediately
            elif not self.digest_mode and scraped_data:
                self.logger.info(f"Processing {len(scraped_data)} URLs with batch processor")
                self.batch_processor.process_batch(
                    all_urls=scraped_data,
                    scraper_name=scraper_name,
                    scraper_key=scraper_key
                )
            
        except Exception as e:
            self.logger.error(f"Error in {scraper_name}: {e}", exc_info=True)
            result['error'] = str(e)
        
        finally:
            result['end_time'] = datetime.now()
            result['duration'] = (result['end_time'] - result['start_time']).total_seconds()
        
        return result
    
    # def send_daily_digest(self):
    #     """
    #     Send a single consolidated email with all URLs from all scrapers.
    #     """
    #     if not self.digest_urls:
    #         self.logger.info("No URLs collected for digest email")
    #         return
    
    # def send_daily_digest(self):
    #     """
    #     Send a single consolidated email with all URLs from all scrapers.
    #     Even sends when no new URLs found.
    #     """
    #     # Send email even if no URLs (to notify user that scrapers ran)
    #     total_urls = len(self.digest_urls)
        
    #     self.logger.info(f"Sending daily digest with {total_urls} total URLs from {len(self.digest_by_scraper)} scrapers")
        
    #     # Create digest email subject
    #     # if total_urls > 0:
    #     #     subject = f"Daily Class Action Digest - {total_urls} New URLs"
    #     # else:
    #     #     subject = "Daily Class Action Digest - No New URLs Found"
        
    #     # Create digest email subject
    #     subject = "Daily Mass Arbitration Report"
        
    #     # Build email body with organized sections
    #     email_body = self._build_digest_email_body()

        
    #     # Send using notifier
    #     from core.notifier import EmailNotifier
    #     notifier = EmailNotifier()
        
    #     success = notifier.send_digest_email(
    #         subject=subject,
    #         body=email_body,
    #         digest_data=self.digest_by_scraper,
    #         total_urls=len(self.digest_urls)
    #     )
        
    #     if success:
    #         self.logger.info("Daily digest email sent successfully")
            
    #         # Update last run times for all scrapers
    #         for scraper_name, urls in self.digest_by_scraper.items():
    #             # Find scraper key
    #             scraper_key = None
    #             for category, scrapers in self.scraper_registry.items():
    #                 for key, info in scrapers.items():
    #                     if info['name'] == scraper_name:
    #                         scraper_key = key
    #                         break
                
    #             if scraper_key:
    #                 self.batch_processor.update_last_run_time(
    #                     scraper_key=scraper_key,
    #                     scraper_name=scraper_name,
    #                     urls_found=len(urls),
    #                     success=True
    #                 )
    #     else:
    #         self.logger.error("❌ Failed to send daily digest email")
    
    def send_daily_digest(self):
        """
        Send a single consolidated email with all URLs from all scrapers.
        Even sends when no new URLs found.
        """
        total_urls = len(self.digest_urls)
        
        self.logger.info(f"Sending daily digest with {total_urls} total URLs from {len(self.digest_by_scraper)} scrapers")
        
        # Create digest email subject
        subject = self.custom_email_subject or "Daily Mass Arbitration Links"
        
        # Build email body with organized sections
        email_body = self._build_digest_email_body()
        
        # Send using notifier
        from core.notifier import EmailNotifier
        notifier = EmailNotifier()
        
        success = notifier.send_digest_email(
            subject=subject,
            body=email_body,
            digest_data=self.digest_by_scraper,
            total_urls=len(self.digest_urls)
        )
        
        if success:
            self.logger.info("Daily digest email sent successfully")
            
            # CRITICAL: Update last run times for ALL scrapers
            # Use digest_by_scraper + scrapers that found 0 URLs
            from config.settings import SCRAPER_REGISTRY
            
            # Track which scrapers we've updated
            updated_scrapers = set()
            
            # First, update scrapers that found URLs
            for scraper_name, urls in self.digest_by_scraper.items():
                scraper_key = self._find_scraper_key(scraper_name)
                
                if scraper_key:
                    self.batch_processor.update_last_run_time(
                        scraper_key=scraper_key,
                        scraper_name=scraper_name,
                        urls_found=len(urls),
                        success=True
                    )
                    updated_scrapers.add(scraper_key)
                    self.logger.info(f"Updated last-run time for {scraper_name} ({len(urls)} URLs)")
            
            # Then update all other scrapers that ran but found 0 URLs
            for category, scrapers in self.scraper_registry.items():
                for scraper_key, scraper_info in scrapers.items():
                    if scraper_key not in updated_scrapers:
                        scraper_name = scraper_info['name']
                        self.batch_processor.update_last_run_time(
                            scraper_key=scraper_key,
                            scraper_name=scraper_name,
                            urls_found=0,
                            success=True
                        )
                        self.logger.info(f"Updated last-run time for {scraper_name} (0 URLs)")
            
            self.logger.info("All last-run times updated successfully")
            # Force save the batch processor's last run times
            self.batch_processor.save_last_run_times()
            self.logger.info("Batch processor last-run times saved to disk")
        else:
            self.logger.error("Failed to send daily digest email")


    def _find_scraper_key(self, scraper_name: str) -> str:
        """
        Find scraper key from scraper name.
        
        Args:
            scraper_name: Display name of scraper
            
        Returns:
            str: Scraper key or None
        """
        from config.settings import SCRAPER_REGISTRY
        
        for category, scrapers in self.scraper_registry.items():
            for key, info in scrapers.items():
                if info['name'] == scraper_name:
                    return key
        return None
 
    # def _build_digest_email_body(self) -> str:
    #     """
    #     Build a nicely formatted email body for the digest.
        
    #     Returns:
    #         str: HTML email body
    #     """
    #     html = """
    #     <html>
    #     <head>
    #         <style>
    #             body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
    #             .header { background: #2c3e50; color: white; padding: 20px; text-align: center; }
    #             .summary { background: #ecf0f1; padding: 15px; margin: 20px 0; border-radius: 5px; }
    #             .scraper-section { margin: 20px 0; padding: 15px; border-left: 4px solid #3498db; background: #f9f9f9; }
    #             .scraper-title { color: #2c3e50; font-size: 18px; font-weight: bold; margin-bottom: 10px; }
    #             .url-list { list-style: none; padding: 0; }
    #             .url-item { margin: 8px 0; padding: 8px; background: white; border-radius: 3px; }
    #             .url-link { color: #3498db; text-decoration: none; }
    #             .url-link:hover { text-decoration: underline; }
    #             .footer { text-align: center; color: #7f8c8d; margin-top: 30px; padding-top: 20px; border-top: 1px solid #bdc3c7; }
    #         </style>
    #     </head>
    #     <body>
    #         <div class="header">
    #             <h1>📧 Daily Class Action Digest</h1>
    #             <p>{date}</p>
    #         </div>
            
    #         <div class="summary">
    #             <h2>📊 Summary</h2>
    #             <p><strong>Total URLs:</strong> {total_urls}</p>
    #             <p><strong>Scrapers Run:</strong> {scraper_count}</p>
    #             <p><strong>Date:</strong> {date}</p>
    #         </div>
            
    #         {scraper_sections}
            
    #         <div class="footer">
    #             <p>Automated by Mass ARB Scraper System</p>
    #             <p>Generated at {timestamp}</p>
    #         </div>
    #     </body>
    #     </html>
    #     """
    
    def _build_digest_email_body(self) -> str:
        """
        Build a nicely formatted email body for the digest.
        
        Returns:
            str: HTML email body
        """
        
        # Handle case when no new URLs found
        if not self.digest_urls and not self.digest_by_scraper:
            # return """
            # <html>
            # <body style="font-family: Arial, sans-serif;">
            #     <div style="background: #2c3e50; color: white; padding: 20px; text-align: center;">
            #         <h1>{self.custom_email_subject or "Daily Mass Arbitration Report"}</h1>
            #         <p>{date}</p>
            #     </div>
                
            #     <div style="background: #ecf0f1; padding: 15px; margin: 20px; border-radius: 5px;">
            #         <h2>No New URLs Found</h2>
            #         <p><strong>All 14 scrapers ran successfully, but no new URLs were found.</strong></p>
            #         <p>This means either:</p>
            #         <ul>
            #             <li>No new lawsuits/cases were posted in the last 24 hours</li>
            #             <li>All found URLs were already sent in previous runs</li>
            #         </ul>
            #         <p><em>Next run will check for new content again.</em></p>
            #     </div>
                
            #     <div style="text-align: center; color: #7f8c8d; margin-top: 30px; padding-top: 20px; border-top: 1px solid #bdc3c7;">
            #         <p>Automated by Mass ARB Scraper System</p>
            #         <p>Generated at {timestamp}</p>
            #     </div>
            # </body>
            # </html>
            # """.format(
            #     date=datetime.now().strftime('%B %d, %Y'),
            #     timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # )
            
            date_str = datetime.now().strftime('%B %d, %Y')
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            email_title = self.custom_email_subject or "Daily Mass Arbitration Report"

            return f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <div style="background: #2c3e50; color: white; padding: 20px; text-align: center;">
                    <h1>{email_title}</h1>
                    <p>{date_str}</p>
                </div>
                
                <div style="background: #ecf0f1; padding: 15px; margin: 20px; border-radius: 5px;">
                    <h2>No New URLs Found</h2>
                    <p><strong>All scrapers ran successfully, but no new URLs were found.</strong></p>
                    <p>This means either:</p>
                    <ul>
                        <li>No new lawsuits/cases were posted in the last 24 hours</li>
                        <li>All found URLs were already sent in previous runs</li>
                    </ul>
                    <p><em>Next run will check for new content again.</em></p>
                </div>
                
                <div style="text-align: center; color: #7f8c8d; margin-top: 30px; padding-top: 20px; border-top: 1px solid #bdc3c7;">
                    <p>Automated by Mass ARB Scraper System</p>
                    <p>Generated at {timestamp_str}</p>
                </div>
            </body>
            </html>
            """
        
        # Build scraper sections
        scraper_sections = ""
        for scraper_name, urls in sorted(self.digest_by_scraper.items()):
            if not urls:
                continue
            
            # Build URL list
            url_items = ""
            for url_data in urls:
                # Handle both dict and string formats
                if isinstance(url_data, dict):
                    url = url_data.get('url', str(url_data))
                    # title = url_data.get('title', 'Untitled')
                else:
                    url = str(url_data)
                    # title = 'Link'
                
                # url_items += f'<li class="url-item">📎 <a href="{url}" class="url-link">{title}</a></li>\n'
                url_items += f'<li class="url-item">📎 <a href="{url}" class="url-link">{url}</a></li>\n'
            
            scraper_sections += f"""
            <div class="scraper-section">
                <div class="scraper-title">🏢 {scraper_name} ({len(urls)} URLs)</div>
                <ul class="url-list">
                    {url_items}
                </ul>
            </div>
            """
        
        # Fill in template
        date_str = datetime.now().strftime('%B %d, %Y')
        timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        email_title = self.custom_email_subject or "Daily Mass Arbitration Report" 
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .header {{ background: #2c3e50; color: white; padding: 20px; text-align: center; }}
                .summary {{ background: #ecf0f1; padding: 15px; margin: 20px 0; border-radius: 5px; }}
                .scraper-section {{ margin: 20px 0; padding: 15px; border-left: 4px solid #3498db; background: #f9f9f9; }}
                .scraper-title {{ color: #2c3e50; font-size: 18px; font-weight: bold; margin-bottom: 10px; }}
                .url-list {{ list-style: none; padding: 0; }}
                .url-item {{ margin: 8px 0; padding: 8px; background: white; border-radius: 3px; }}
                .url-link {{ color: #3498db; text-decoration: none; }}
                .url-link:hover {{ text-decoration: underline; }}
                .footer {{ text-align: center; color: #7f8c8d; margin-top: 30px; padding-top: 20px; border-top: 1px solid #bdc3c7; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{email_title}</h1>
                <p>{date_str}</p>
            </div>
            
            <div class="summary">
                <h2>📊 Summary</h2>
                <p><strong>Total URLs:</strong> {len(self.digest_urls)}</p>
                <p><strong>Scrapers Run:</strong> {len(self.digest_by_scraper)}</p>
                <p><strong>Date:</strong> {date_str}</p>
            </div>
            
            {scraper_sections}
            
            <div class="footer">
                <p>Automated by Mass ARB Scraper System</p>
                <p>Generated at {timestamp_str}</p>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def run_all_scrapers(self):
        """Run all enabled scrapers"""
        self.logger.info("Starting execution of all scrapers")
        self.start_time = datetime.now()
        self.results = {}
        self.digest_urls = []
        self.digest_by_scraper = {}
        
        total_scrapers = 0
        
        # Iterate through all categories and scrapers
        for category, scrapers in self.scraper_registry.items():
            self.logger.info(f"Processing category: {category}")
            
            for scraper_key, scraper_info in scrapers.items():
                # Check if enabled
                if not scraper_info.get('enabled', True):
                    self.logger.info(f"Skipping disabled scraper: {scraper_info['name']}")
                    continue
                
                total_scrapers += 1
                
                # Execute scraper
                result = self._execute_scraper(scraper_key, scraper_info)
                self.results[scraper_key] = result
                
                # Log result
                if result['success']:
                    self.logger.info(
                        f"✅ {result['scraper_name']}: {result['urls_found']} URLs "
                        f"in {result['duration']:.2f}s"
                    )
                else:
                    self.logger.error(
                        f"❌ {result['scraper_name']}: {result['error']}"
                    )
        
        self.end_time = datetime.now()
        self.logger.info(f"Completed execution of {total_scrapers} scrapers")  
        
        # Send daily digest if in digest mode
        if self.digest_mode:
            self.logger.info("Preparing to send daily digest email...")
            self.send_daily_digest()
    
    def get_statistics(self) -> Dict:
        """
        Get execution statistics.
        
        Returns:
            dict: Statistics summary
        """
        total = len(self.results)
        successful = sum(1 for r in self.results.values() if r['success'])
        failed = total - successful
        total_urls = sum(r['urls_found'] for r in self.results.values())
        
        total_duration = 0
        if self.start_time and self.end_time:
            total_duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            'total_scrapers': total,
            'successful': successful,
            'failed': failed,
            'total_urls': total_urls,
            'duration': total_duration,
            'digest_mode': self.digest_mode
        }
    
    def print_summary(self):
        """Print execution summary"""
        stats = self.get_statistics()
        
        print("\n" + "="*70)
        print("  EXECUTION SUMMARY")
        print("="*70)
        
        if not self.results:
            print("  No scrapers were executed")
            print("="*70 + "\n")
            return
        
        # Print individual results
        for scraper_key, result in self.results.items():
            status = "✅" if result['success'] else "❌"
            print(f"  {status} {result['scraper_name']}")
            print(f"     URLs: {result['urls_found']}, Duration: {result['duration']:.2f}s")
            if result['error']:
                print(f"     Error: {result['error']}")
        
        print("\n" + "-"*70)
        print(f"  Total: {stats['total_scrapers']} scrapers")
        print(f"  Successful: {stats['successful']}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Total URLs: {stats['total_urls']}")
        print(f"  Total Duration: {stats['duration']:.2f}s")
        
        if stats['digest_mode']:
            print(f"  📧 Digest Email: Sent with {stats['total_urls']} URLs")
        
        print("="*70 + "\n")
    
    def list_all_scrapers(self):
        """List all available scrapers"""
        print("\n" + "="*70)
        print("  AVAILABLE SCRAPERS")
        print("="*70 + "\n")
        
        for category, scrapers in self.scraper_registry.items():
            print(f"📁 {category.upper().replace('_', ' ')}")
            print("-" * 70)
            
            for scraper_key, info in scrapers.items():
                status = "✅ Enabled" if info.get('enabled', True) else "❌ Disabled"
                print(f"  • {info['name']:<30} [{scraper_key}]")
                print(f"    Status: {status}")
                print(f"    Module: {info['module']}")
            
            print()
        
        print("="*70 + "\n")
