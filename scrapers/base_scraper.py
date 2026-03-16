#!/usr/bin/env python3
"""
Base Scraper Class - Foundation for All Scrapers
Provides common functionality: date extraction, URL filtering, data persistence.
All scrapers inherit from this class.
"""

import requests
import logging
import json
import time
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict

from config.settings import (
    SCRAPER_CONFIG, 
    DATE_FILTER_CONFIG, 
    SCRAPED_DIR, 
    LOGS_DIR,
    is_date_filtering_enabled
)
from utils.date_utils import (
    parse_date, 
    is_newer_than, 
    extract_date_from_html,
    get_common_date_selectors
)


class BaseScraper(ABC):
    """
    Abstract base class for all scrapers.
    Provides common functionality for web scraping with date filtering.
    """
    
    def __init__(self, name: str, base_url: str, scraper_key: str = None):
        """
        Initialize the base scraper.
        
        Args:
            name (str): Human-readable scraper name
            base_url (str): Base URL to scrape
            scraper_key (str): Unique scraper key for tracking (optional)
        """
        self.name = name
        self.base_url = base_url
        self.scraper_key = scraper_key or name.lower().replace(' ', '_')
        
        # Session for connection reuse
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': SCRAPER_CONFIG['user_agent']
        })
        
        # Configuration from settings
        self.max_pages = SCRAPER_CONFIG['max_pages']
        self.request_delay = SCRAPER_CONFIG['request_delay']
        self.max_retries = SCRAPER_CONFIG['max_retries']
        self.timeout = SCRAPER_CONFIG['timeout']
        
        # Date filtering configuration
        self.enable_date_filtering = is_date_filtering_enabled(self.scraper_key)
        self.hours_threshold = DATE_FILTER_CONFIG['hours_threshold']
        self.include_undated = DATE_FILTER_CONFIG['include_undated']
        
        # Data storage
        self.visited_urls = set()
        self.scraped_data = []
        
        # Setup logging
        self.logger = self._setup_logger()
        
        self.logger.info(f"Initialized {self.name} scraper")
        if self.enable_date_filtering:
            self.logger.info(f"Date filtering enabled: last {self.hours_threshold} hours")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for this scraper"""
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # File handler
            log_file = LOGS_DIR / f"{self.scraper_key}.log"
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(
                logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            )
            logger.addHandler(file_handler)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(
                logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            )
            logger.addHandler(console_handler)
        
        return logger
    
    # ========================================================================
    # ABSTRACT METHODS (Must be implemented by subclasses)
    # ========================================================================
    
    @abstractmethod
    def is_valid_url(self, url: str) -> bool:
        """
        Check if URL should be scraped.
        Must be implemented by each scraper.
        
        Args:
            url (str): URL to validate
            
        Returns:
            bool: True if URL should be scraped
        """
        pass
    
    @abstractmethod
    def scrape(self):
        """
        Main scraping logic.
        Must be implemented by each scraper.
        """
        pass
    
    # ========================================================================
    # COMMON URL UTILITIES
    # ========================================================================
    
    def normalize_url(self, url: str) -> Optional[str]:
        """
        Normalize URL for consistent comparison.
        
        Args:
            url (str): URL to normalize
            
        Returns:
            str: Normalized URL or None
        """
        if not url:
            return None
        
        try:
            parsed = urlparse(url)
            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            return normalized.rstrip('/').lower()
        except Exception as e:
            self.logger.warning(f"Could not normalize URL {url}: {e}")
            return None
    
    def is_same_domain(self, url: str) -> bool:
        """
        Check if URL is from the same domain as base_url.
        
        Args:
            url (str): URL to check
            
        Returns:
            bool: True if same domain
        """
        try:
            base_domain = urlparse(self.base_url).netloc
            url_domain = urlparse(url).netloc
            return base_domain == url_domain
        except Exception:
            return False
    
    def make_absolute_url(self, url: str, base: str = None) -> str:
        """
        Convert relative URL to absolute.
        
        Args:
            url (str): URL (relative or absolute)
            base (str): Base URL for resolution (uses self.base_url if None)
            
        Returns:
            str: Absolute URL
        """
        base = base or self.base_url
        return urljoin(base, url)
    
    # ========================================================================
    # HTTP REQUEST UTILITIES
    # ========================================================================
    
    def fetch_page(self, url: str, retry_count: int = 0) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a web page with retry logic.
        
        Args:
            url (str): URL to fetch
            retry_count (int): Current retry attempt
            
        Returns:
            BeautifulSoup: Parsed HTML or None if failed
        """
        try:
            self.logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403 and retry_count < self.max_retries:
                self.logger.warning(f"403 Forbidden (attempt {retry_count + 1}/{self.max_retries})")
                time.sleep(2 ** retry_count)  # Exponential backoff
                return self.fetch_page(url, retry_count + 1)
            else:
                self.logger.error(f"HTTP error for {url}: {e}")
                return None
                
        except requests.exceptions.RequestException as e:
            if retry_count < self.max_retries:
                self.logger.warning(f"Request failed (attempt {retry_count + 1}/{self.max_retries}): {e}")
                time.sleep(2 ** retry_count)
                return self.fetch_page(url, retry_count + 1)
            else:
                self.logger.error(f"Failed to fetch {url} after {self.max_retries} retries: {e}")
                return None
                
        except Exception as e:
            self.logger.error(f"Unexpected error fetching {url}: {e}")
            return None
    
    def add_delay(self):
        """Add delay between requests to be respectful."""
        if self.request_delay > 0:
            time.sleep(self.request_delay)
    
    # ========================================================================
    # DATE EXTRACTION & FILTERING (NEW)
    # ========================================================================
    
    def extract_date_from_page(self, soup: BeautifulSoup, url: str) -> Optional[datetime]:
        """
        Extract publication/update date from page.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            url (str): Page URL (for logging)
            
        Returns:
            datetime: Extracted date or None
        """
        # Use common date selectors
        selectors = get_common_date_selectors()
        
        # Try to extract date
        date = extract_date_from_html(soup, selectors)
        
        if date:
            self.logger.debug(f"Extracted date from {url}: {date.isoformat()}")
        else:
            self.logger.debug(f"No date found for {url}")
        
        return date
    
    def is_recent_content(self, page_date: Optional[datetime], reference_date: Optional[datetime] = None) -> bool:
        """
        Check if content is recent enough to include.
        
        Args:
            page_date (datetime): Date extracted from page
            reference_date (datetime): Reference date to compare against (uses last run if None)
            
        Returns:
            bool: True if content should be included
        """
        # If date filtering is disabled, include everything
        if not self.enable_date_filtering:
            return True
        
        # If no date found, use include_undated setting
        if not page_date:
            return self.include_undated
        
        # If no reference date provided, check against threshold
        if not reference_date:
            from datetime import timedelta
            now = datetime.now(timezone.utc)
            threshold_date = now - timedelta(hours=self.hours_threshold)
            reference_date = threshold_date
        
        # Check if page is newer than reference
        is_recent = is_newer_than(page_date, reference_date)
        
        if not is_recent:
            self.logger.debug(f"Skipping old content (date: {page_date.isoformat()})")
        
        return is_recent
    
    def should_include_url(self, url: str, soup: BeautifulSoup = None, reference_date: Optional[datetime] = None) -> bool:
        """
        Comprehensive check if URL should be included.
        Checks: URL validity, duplicates, and date filtering.
        
        Args:
            url (str): URL to check
            soup (BeautifulSoup): Parsed page (for date extraction)
            reference_date (datetime): Reference date for comparison
            
        Returns:
            bool: True if URL should be included
        """
        # Check if valid URL
        if not self.is_valid_url(url):
            return False
        
        # Check if already visited
        normalized = self.normalize_url(url)
        if normalized and normalized in self.visited_urls:
            return False
        
        # Check date filtering (if enabled and soup provided)
        if self.enable_date_filtering and soup:
            page_date = self.extract_date_from_page(soup, url)
            if not self.is_recent_content(page_date, reference_date):
                return False
        
        return True
    
    # ========================================================================
    # DATA EXTRACTION HELPERS
    # ========================================================================
    
    def extract_title(self, soup: BeautifulSoup) -> str:
        """
        Extract page title.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            
        Returns:
            str: Page title or empty string
        """
        try:
            # Try h1 first
            h1 = soup.find('h1')
            if h1:
                return h1.get_text(strip=True)
            
            # Try title tag
            title = soup.find('title')
            if title:
                return title.get_text(strip=True)
            
            return ""
        except Exception as e:
            self.logger.warning(f"Could not extract title: {e}")
            return ""
    
    def extract_description(self, soup: BeautifulSoup, max_length: int = 200) -> str:
        """
        Extract page description.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            max_length (int): Maximum description length
            
        Returns:
            str: Page description or empty string
        """
        try:
            # Try meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                desc = meta_desc['content'].strip()
                return desc[:max_length]
            
            # Try first paragraph
            p = soup.find('p')
            if p:
                desc = p.get_text(strip=True)
                return desc[:max_length]
            
            return ""
        except Exception as e:
            self.logger.warning(f"Could not extract description: {e}")
            return ""
    
    def create_case_data(self, url: str, soup: BeautifulSoup = None, **kwargs) -> Dict:
        """
        Create standardized case data dictionary.
        
        Args:
            url (str): Case URL
            soup (BeautifulSoup): Parsed HTML (optional)
            **kwargs: Additional data fields
            
        Returns:
            dict: Standardized case data
        """
        data = {
            'url': url,
            'scraped_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Extract common fields if soup provided
        if soup:
            data['title'] = self.extract_title(soup)
            data['description'] = self.extract_description(soup)
            
            # Extract date
            page_date = self.extract_date_from_page(soup, url)
            if page_date:
                data['date'] = page_date.isoformat()
        
        # Add any additional fields
        data.update(kwargs)
        
        return data
    
    # ========================================================================
    # DATA PERSISTENCE
    # ========================================================================
    
    def save_data(self, filename: str = None) -> Path:
        """
        Save scraped data to JSON file.
        
        Args:
            filename (str): Optional filename (auto-generated if None)
            
        Returns:
            Path: Path to saved file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.scraper_key}_data_{timestamp}.json"
        
        filepath = SCRAPED_DIR / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.scraped_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Data saved to {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
            raise
    
    # ========================================================================
    # MAIN EXECUTION
    # ========================================================================
    
    def run(self, reference_date: Optional[datetime] = None) -> List[Dict]:
        """
        Main execution method.
        
        Args:
            reference_date (datetime): Reference date for filtering (uses last run if None)
            
        Returns:
            List[Dict]: Scraped data
        """
        self.logger.info(f"Starting {self.name} scraper...")
        start_time = time.time()
        
        try:
            # Run the scraping logic (implemented by subclass)
            self.scrape()
            
            # Save data
            if self.scraped_data:
                self.save_data()
            
            duration = time.time() - start_time
            
            self.logger.info(f"Scraping completed!")
            self.logger.info(f"Total items scraped: {len(self.scraped_data)}")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            
            return self.scraped_data
            
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
            raise
        finally:
            self.session.close()
    
    def get_stats(self) -> Dict:
        """
        Get scraping statistics.
        
        Returns:
            dict: Statistics
        """
        return {
            'scraper_name': self.name,
            'scraper_key': self.scraper_key,
            'base_url': self.base_url,
            'total_scraped': len(self.scraped_data),
            'total_visited': len(self.visited_urls),
            'date_filtering_enabled': self.enable_date_filtering,
            'hours_threshold': self.hours_threshold if self.enable_date_filtering else None
        }