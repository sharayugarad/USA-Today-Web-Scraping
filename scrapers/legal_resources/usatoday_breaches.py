"""
USA Today Healthcare Data Breaches Scraper - WITH DEDUPLICATION
Scrapes ALL pages and returns unique breaches only
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
import time
import hashlib


class USATodayBreachesScraper:
    """Scraper for USA Today Healthcare Data Breaches with deduplication"""
    
    def __init__(self):
        self.base_url = "https://data.usatoday.com/health-care-data-breaches/"
        self.source_name = "USA Today Healthcare Breaches"
        self.category = "legal_resources"
        
    def setup_driver(self):
        """Setup Chrome driver with headless options"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    
    def parse_date(self, date_str):
        """Parse date string into datetime object"""
        if not date_str:
            return None
        
        date_str = date_str.strip()
        
        formats = [
            '%b. %d, %Y',
            '%B %d, %Y',
            '%m/%d/%Y',
            '%Y-%m-%d',
            '%b %d, %Y',
            '%m-%d-%Y',
            '%d/%m/%Y'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def is_recent(self, date_obj, days=30):
        """Check if date is recent"""
        if not date_obj:
            return True
        
        future_cutoff = datetime.now() + timedelta(days=30)
        past_cutoff = datetime.now() - timedelta(days=days)
        
        return past_cutoff <= date_obj <= future_cutoff
    
    def create_breach_hash(self, company, state, breach_date, people_affected):
        """
        Create unique hash for a breach to detect duplicates
        Uses: company + state + date + people affected
        """
        unique_string = f"{company}|{state}|{breach_date}|{people_affected}".lower()
        return hashlib.md5(unique_string.encode()).hexdigest()
    
    def click_next_page(self, driver):
        """Try to click the 'Next' button to go to next page"""
        try:
            next_button_selectors = [
                "button[aria-label='Next page']",
                "a[aria-label='Next page']",
                ".pagination .next",
                "button.next-page",
                ".pagination a:last-child"
            ]
            
            for selector in next_button_selectors:
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, selector)
                    if next_button and next_button.is_displayed() and next_button.is_enabled():
                        next_button.click()
                        time.sleep(3)
                        return True
                except:
                    continue
            
            # Try finding by text
            try:
                next_links = driver.find_elements(By.LINK_TEXT, "Next")
                if not next_links:
                    next_links = driver.find_elements(By.PARTIAL_LINK_TEXT, "Next")
                
                for link in next_links:
                    if link.is_displayed() and link.is_enabled():
                        link.click()
                        time.sleep(3)
                        return True
            except:
                pass
            
            return False
            
        except Exception as e:
            print(f"[{self.source_name}] Could not click next page: {e}")
            return False
    
    def scrape(self, days_back=30, max_pages=10):
        """
        Scrape healthcare data breaches with pagination and deduplication
        
        Args:
            days_back: Number of days to look back (default: 30)
            max_pages: Maximum number of pages to scrape (default: 10)
            
        Returns:
            list: List of dictionaries with unique breach information
        """
        driver = None
        results = []
        seen_hashes = set()  # Track unique breaches by hash
        total_duplicates = 0
        
        try:
            driver = self.setup_driver()
            print(f"[{self.source_name}] Loading {self.base_url}...")
            
            driver.get(self.base_url)
            time.sleep(5)
            
            page_number = 1
            
            while page_number <= max_pages:
                print(f"[{self.source_name}] Scraping page {page_number}...")
                
                # Find table rows
                breach_elements = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                
                if not breach_elements:
                    print(f"[{self.source_name}] No breach items found on page {page_number}")
                    break
                
                print(f"[{self.source_name}] Found {len(breach_elements)} rows on page {page_number}")
                
                page_results = 0
                page_duplicates = 0
                
                # Process each breach item
                for idx, element in enumerate(breach_elements):
                    try:
                        cells = element.find_elements(By.TAG_NAME, 'td')
                        
                        if len(cells) < 4:
                            continue
                        
                        # Extract all columns
                        # Column order: Company | Type | State | Date | People | Breach Type | Source
                        company = cells[0].text.strip() if len(cells) > 0 else ""
                        company_type = cells[1].text.strip() if len(cells) > 1 else ""
                        state = cells[2].text.strip() if len(cells) > 2 else ""
                        breach_date = cells[3].text.strip() if len(cells) > 3 else ""
                        people_affected = cells[4].text.strip() if len(cells) > 4 else ""
                        breach_type = cells[5].text.strip() if len(cells) > 5 else ""
                        breach_source = cells[6].text.strip() if len(cells) > 6 else ""
                        
                        if not company:
                            continue
                        
                        # Create unique hash to detect duplicates
                        breach_hash = self.create_breach_hash(company, state, breach_date, people_affected)
                        
                        # Skip if we've already seen this exact breach
                        if breach_hash in seen_hashes:
                            page_duplicates += 1
                            continue
                        
                        seen_hashes.add(breach_hash)
                        
                        # Parse date
                        date_obj = self.parse_date(breach_date)
                        
                        # Filter by date
                        if self.is_recent(date_obj, days=days_back):
                            # Create unique identifier using hash
                            breach_id = breach_hash[:12]
                            
                            title = f"{company} - {state} - {people_affected} people affected"
                            
                            description = (
                                f"Company Type: {company_type} | "
                                f"State: {state} | "
                                f"Breach Date: {breach_date} | "
                                f"People Affected: {people_affected} | "
                                f"Breach Type: {breach_type} | "
                                f"Breach Source: {breach_source}"
                            )
                            
                            breach_info = {
                                'title': title,
                                'url': f"{self.base_url}#{breach_id}",
                                'description': description,
                                'date': date_obj.strftime('%Y-%m-%d') if date_obj else breach_date,
                                'source': self.source_name,
                                'breach_hash': breach_hash,
                                # Detailed fields for table format
                                'company': company,
                                'state': state,
                                'company_type': company_type,
                                'breach_date': breach_date,
                                'people_affected': people_affected,
                                'breach_type': breach_type,
                                'breach_source': breach_source,
                                'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            results.append(breach_info)
                            page_results += 1
                    
                    except Exception as e:
                        continue
                
                total_duplicates += page_duplicates
                print(f"[{self.source_name}] Page {page_number}: {page_results} unique, {page_duplicates} duplicates")
                
                # Try to go to next page
                if page_number < max_pages:
                    if not self.click_next_page(driver):
                        print(f"[{self.source_name}] No more pages available")
                        break
                    page_number += 1
                else:
                    print(f"[{self.source_name}] Reached max pages ({max_pages})")
                    break
            
            print(f"[{self.source_name}] Total: {len(results)} unique breaches from {page_number} page(s)")
            print(f"[{self.source_name}] Filtered out {total_duplicates} duplicate entries")
            
        except Exception as e:
            print(f"[{self.source_name}] ERROR: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            if driver:
                driver.quit()
        
        return results
    
    def run(self):
        """Run method for orchestrator compatibility"""
        results = self.scrape(days_back=45, max_pages=10)
        
        urls = []
        for item in results:
            urls.append({
                'url': item['url'],
                'title': item['title'],
                'description': item.get('description', ''),
                'date': item.get('date', ''),
                'source': self.source_name,
                'breach_hash': item.get('breach_hash', ''),
                # Include table data
                'company': item.get('company', ''),
                'state': item.get('state', ''),
                'company_type': item.get('company_type', ''),
                'breach_date': item.get('breach_date', ''),
                'people_affected': item.get('people_affected', ''),
                'breach_type': item.get('breach_type', ''),
                'breach_source': item.get('breach_source', '')
            })
        
        return urls


def main():
    """Test the scraper"""
    scraper = USATodayBreachesScraper()
    results = scraper.scrape(days_back=45, max_pages=10)
    
    print(f"\n{'='*80}")
    print(f"Found {len(results)} UNIQUE healthcare breaches across all pages:")
    print(f"{'='*80}\n")
    
    # Print in table format
    print(f"{'Company':<40} {'State':<6} {'Date':<15} {'People':<10}")
    print("-" * 80)
    
    for item in results[:20]:
        company = item['company'][:38]
        state = item['state']
        date = item['breach_date']
        people = item['people_affected']
        
        print(f"{company:<40} {state:<6} {date:<15} {people:<10}")
    
    if len(results) > 20:
        print(f"\n... and {len(results) - 20} more unique breaches")


if __name__ == "__main__":
    main()