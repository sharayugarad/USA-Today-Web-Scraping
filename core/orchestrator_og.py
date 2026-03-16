#!/usr/bin/env python3
"""
Main Orchestrator for Mass ARB Scraper System
Manages all scrapers and coordinates batch processing.
"""

import importlib
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

from config.settings import SCRAPER_REGISTRY, get_scraper_info, get_all_enabled_scrapers
from core.batch_processor import BatchProcessor


class ScraperOrchestrator:
    """
    Orchestrates all scrapers and manages batch sending.
    """
    
    def __init__(self, batch_size=None, delay_minutes=None):
        """
        Initialize the orchestrator.
        
        Args:
            batch_size (int): Optional batch size override
            delay_minutes (int): Optional delay override
        """
        self.results = {}
        self.batch_processor = BatchProcessor(
            batch_size=batch_size,
            delay_minutes=delay_minutes
        )
        self.logger = logging.getLogger('Orchestrator')
        self.logger.info("[ORCHESTRATOR] Initialized successfully")
    
    def run_scraper(self, category: str, scraper_key: str) -> Optional[List]:
        """
        Run a single scraper by category and key.
        
        Args:
            category (str): Scraper category ('law_firms', 'legal_resources', 'government')
            scraper_key (str): Scraper key (e.g., 'clarkson', 'ny_ag')
            
        Returns:
            List: List of scraped URLs or None if failed
        """
        if category not in SCRAPER_REGISTRY:
            self.logger.error(f"Unknown category: {category}")
            return None
        
        if scraper_key not in SCRAPER_REGISTRY[category]:
            self.logger.error(f"Unknown scraper: {scraper_key} in category {category}")
            return None
        
        scraper_info = SCRAPER_REGISTRY[category][scraper_key]
        
        # Check if scraper is enabled
        if not scraper_info.get('enabled', True):
            self.logger.info(f"Skipping disabled scraper: {scraper_info['name']}")
            self.results[scraper_key] = {
                'success': False,
                'count': 0,
                'message': 'Disabled'
            }
            return None
        
        try:
            self.logger.info(f"="*60)
            self.logger.info(f"Starting scraper: {scraper_info['name']}")
            self.logger.info(f"URL: {scraper_info['url']}")
            self.logger.info(f"="*60)
            
            # Import scraper module dynamically
            module = importlib.import_module(scraper_info['module'])
            
            # Run scraper's main function
            start_time = time.time()
            urls = module.main()
            duration = time.time() - start_time
            
            # Process results
            if urls:
                url_count = len(urls)
                self.logger.info(f"[SUCCESS] {scraper_info['name']} scraped {url_count} URLs in {duration:.2f}s")
                
                # Process with batch processor
                self.batch_processor.process_batch(urls, scraper_info['name'])
                
                self.results[scraper_key] = {
                    'success': True,
                    'count': url_count,
                    'duration': duration,
                    'message': f'{url_count} URLs scraped'
                }
                
                return urls
            else:
                self.logger.info(f"[INFO] {scraper_info['name']} found no URLs in {duration:.2f}s")
                self.results[scraper_key] = {
                    'success': True,
                    'count': 0,
                    'duration': duration,
                    'message': 'No URLs found'
                }
                return []
            
        except ModuleNotFoundError as e:
            self.logger.error(f"Could not import module {scraper_info['module']}: {e}")
            self.results[scraper_key] = {
                'success': False,
                'count': 0,
                'error': f'Module not found: {e}'
            }
            return None
            
        except AttributeError as e:
            self.logger.error(f"Module {scraper_info['module']} has no main() function: {e}")
            self.results[scraper_key] = {
                'success': False,
                'count': 0,
                'error': f'No main() function: {e}'
            }
            return None
            
        except Exception as e:
            self.logger.error(f"Error running {scraper_info['name']}: {e}")
            self.results[scraper_key] = {
                'success': False,
                'count': 0,
                'error': str(e)
            }
            return None
    
    def run_all_scrapers(self) -> Dict:
        """
        Run all enabled scrapers sequentially.
        
        Returns:
            Dict: Results dictionary with scraper outcomes
        """
        self.logger.info("="*60)
        self.logger.info("STARTING MASS ARB SCRAPER SYSTEM")
        self.logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("="*60)
        
        enabled_scrapers = get_all_enabled_scrapers()
        total_scrapers = len(enabled_scrapers)
        
        self.logger.info(f"Total enabled scrapers: {total_scrapers}")
        
        for idx, (category, scraper_key, scraper_info) in enumerate(enabled_scrapers, 1):
            self.logger.info(f"\n[{idx}/{total_scrapers}] Running: {scraper_info['name']}")
            self.run_scraper(category, scraper_key)
            
            # Small delay between scrapers to be respectful
            if idx < total_scrapers:
                time.sleep(2)
        
        self.logger.info("\n" + "="*60)
        self.logger.info("ALL SCRAPERS COMPLETED")
        self.logger.info("="*60)
        
        return self.results
    
    def run_category(self, category: str) -> Dict:
        """
        Run all scrapers in a specific category.
        
        Args:
            category (str): Category name ('law_firms', 'legal_resources', 'government')
            
        Returns:
            Dict: Results dictionary
        """
        if category not in SCRAPER_REGISTRY:
            self.logger.error(f"Unknown category: {category}")
            return {}
        
        self.logger.info("="*60)
        self.logger.info(f"RUNNING CATEGORY: {category.upper()}")
        self.logger.info("="*60)
        
        scrapers = SCRAPER_REGISTRY[category]
        enabled_scrapers = [(key, info) for key, info in scrapers.items() if info.get('enabled', True)]
        total_scrapers = len(enabled_scrapers)
        
        self.logger.info(f"Total scrapers in category: {total_scrapers}")
        
        for idx, (scraper_key, scraper_info) in enumerate(enabled_scrapers, 1):
            self.logger.info(f"\n[{idx}/{total_scrapers}] Running: {scraper_info['name']}")
            self.run_scraper(category, scraper_key)
            
            if idx < total_scrapers:
                time.sleep(2)
        
        self.logger.info("\n" + "="*60)
        self.logger.info(f"CATEGORY {category.upper()} COMPLETED")
        self.logger.info("="*60)
        
        return self.results
    
    def run_specific_scraper(self, scraper_name: str) -> Optional[List]:
        """
        Run a specific scraper by name or key.
        
        Args:
            scraper_name (str): Scraper name or key (e.g., 'clarkson' or 'Clarkson Law Firm')
            
        Returns:
            List: Scraped URLs or None if not found
        """
        scraper_name_lower = scraper_name.lower()
        
        # Search for scraper in registry
        for category, scrapers in SCRAPER_REGISTRY.items():
            for key, info in scrapers.items():
                if (key.lower() == scraper_name_lower or 
                    info['name'].lower() == scraper_name_lower):
                    self.logger.info(f"Found scraper: {info['name']} in category {category}")
                    return self.run_scraper(category, key)
        
        self.logger.error(f"Scraper not found: {scraper_name}")
        return None
    
    def print_summary(self):
        """Print execution summary."""
        print("\n" + "="*60)
        print("EXECUTION SUMMARY")
        print("="*60)
        
        if not self.results:
            print("No scrapers executed.")
            print("="*60 + "\n")
            return
        
        total = len(self.results)
        successful = sum(1 for r in self.results.values() if r.get('success', False))
        failed = total - successful
        total_urls = sum(r.get('count', 0) for r in self.results.values())
        
        print(f"\nOverall Statistics:")
        print(f"  Total Scrapers: {total}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Total URLs: {total_urls}")
        
        print("\nDetailed Results:")
        print("-" * 60)
        
        for scraper_key, result in self.results.items():
            # Get scraper info
            scraper_info = get_scraper_info(scraper_key)
            name = scraper_info['name'] if scraper_info else scraper_key
            
            if result.get('success', False):
                status = "✓"
                count = result.get('count', 0)
                duration = result.get('duration', 0)
                message = result.get('message', '')
                print(f"  {status} {name}: {count} URLs ({duration:.2f}s) - {message}")
            else:
                status = "✗"
                error = result.get('error', result.get('message', 'Unknown error'))
                print(f"  {status} {name}: FAILED - {error}")
        
        print("="*60 + "\n")
    
    def get_statistics(self) -> Dict:
        """
        Get detailed statistics.
        
        Returns:
            Dict: Statistics dictionary
        """
        if not self.results:
            return {
                'total_scrapers': 0,
                'successful': 0,
                'failed': 0,
                'total_urls': 0,
                'results': {}
            }
        
        total = len(self.results)
        successful = sum(1 for r in self.results.values() if r.get('success', False))
        failed = total - successful
        total_urls = sum(r.get('count', 0) for r in self.results.values())
        
        return {
            'total_scrapers': total,
            'successful': successful,
            'failed': failed,
            'total_urls': total_urls,
            'timestamp': datetime.now().isoformat(),
            'results': self.results
        }
    
    def list_all_scrapers(self):
        """List all available scrapers."""
        print("\n" + "="*60)
        print("AVAILABLE SCRAPERS")
        print("="*60)
        
        for category, scrapers in SCRAPER_REGISTRY.items():
            print(f"\n{category.upper().replace('_', ' ')}:")
            print("-" * 60)
            
            for key, info in scrapers.items():
                status = "✓" if info.get('enabled', True) else "✗"
                print(f"  {status} {key:20} - {info['name']}")
                print(f"     URL: {info['url']}")
        
        print("\n" + "="*60)
        
        # Summary
        total = sum(len(scrapers) for scrapers in SCRAPER_REGISTRY.values())
        enabled = sum(
            1 for scrapers in SCRAPER_REGISTRY.values() 
            for info in scrapers.values() 
            if info.get('enabled', True)
        )
        
        print(f"\nTotal: {total} scrapers ({enabled} enabled)")
        print("="*60 + "\n")


def main():
    """
    Main function for testing orchestrator.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Mass ARB Scraper Orchestrator')
    parser.add_argument('--list', action='store_true', help='List all available scrapers')
    parser.add_argument('--all', action='store_true', help='Run all scrapers')
    parser.add_argument('--category', type=str, help='Run scrapers in category')
    parser.add_argument('--scraper', type=str, help='Run specific scraper')
    parser.add_argument('--batch-size', type=int, help='Batch size override')
    parser.add_argument('--delay', type=int, help='Delay between batches (minutes)')
    
    args = parser.parse_args()
    
    # Initialize orchestrator
    orchestrator = ScraperOrchestrator(
        batch_size=args.batch_size,
        delay_minutes=args.delay
    )
    
    # List scrapers
    if args.list:
        orchestrator.list_all_scrapers()
        return
    
    # Run scrapers
    if args.all:
        orchestrator.run_all_scrapers()
    elif args.category:
        orchestrator.run_category(args.category)
    elif args.scraper:
        orchestrator.run_specific_scraper(args.scraper)
    else:
        parser.print_help()
        return
    
    # Print summary
    orchestrator.print_summary()


if __name__ == '__main__':
    main()