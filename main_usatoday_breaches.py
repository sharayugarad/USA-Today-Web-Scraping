#!/usr/bin/env python3
"""
USA Today Healthcare Breaches - WITH CLICKABLE LINKS
Company names link to USA Today search results
"""

import json
from datetime import datetime
from pathlib import Path
from config.settings import USATODAY_REGISTRY
from core.notifier import EmailNotifier


class USATodayTracker:
    """Track sent USA Today breaches"""
    
    def __init__(self):
        self.tracking_file = Path('data/scraped/usatoday_sent_urls.json')
        self.sent_urls = self.load_sent_urls()
    
    def load_sent_urls(self):
        if self.tracking_file.exists():
            try:
                with open(self.tracking_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"[TRACKER] Loaded {len(data)} previously sent breaches")
                    return set(data)
            except:
                pass
        return set()
    
    def save_sent_urls(self):
        self.tracking_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.tracking_file, 'w', encoding='utf-8') as f:
            json.dump(list(self.sent_urls), f, indent=2)
        print(f"[TRACKER] Saved {len(self.sent_urls)} breach identifiers to tracking file")
    
    def filter_new_urls(self, urls):
        """Filter using breach_hash if available, fallback to URL"""
        new_urls = []
        duplicate_count = 0
        
        for url_dict in urls:
            identifier = url_dict.get('breach_hash', url_dict.get('url', ''))
            
            if identifier and identifier not in self.sent_urls:
                new_urls.append(url_dict)
            else:
                duplicate_count += 1
        
        print(f"[TRACKER] Found {len(new_urls)} new breaches, {duplicate_count} duplicates")
        return new_urls
    
    def mark_as_sent(self, urls):
        """Mark using breach_hash if available, fallback to URL"""
        for url_dict in urls:
            identifier = url_dict.get('breach_hash', url_dict.get('url', ''))
            if identifier:
                self.sent_urls.add(identifier)
        self.save_sent_urls()


def create_table_email(breaches):
    """Create HTML email with table and clickable company links"""
    body = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            h2 {{ color: #2c3e50; }}
            .info {{ 
                background-color: #d1ecf1; 
                padding: 12px; 
                margin: 15px 0; 
                border-left: 4px solid #0c5460;
                font-size: 0.9em;
            }}
            .breach-table {{ 
                width: 100%; 
                border-collapse: collapse; 
                margin: 15px 0;
                font-size: 0.9em;
            }}
            .breach-table th {{ 
                background-color: #007bff; 
                color: white; 
                padding: 10px; 
                text-align: left;
                font-weight: bold;
            }}
            .breach-table td {{ 
                padding: 8px; 
                border: 1px solid #dee2e6;
            }}
            .breach-table tr:nth-child(even) {{ 
                background-color: #f8f9fa; 
            }}
            .breach-table tr:hover {{ 
                background-color: #e9ecef; 
            }}
            .breach-link {{
                color: #007bff;
                text-decoration: none;
                font-weight: bold;
            }}
            .breach-link:hover {{
                text-decoration: underline;
                color: #0056b3;
            }}
            .footer {{ margin-top: 30px; padding-top: 20px; border-top: 2px solid #dee2e6; color: #6c757d; }}
        </style>
    </head>
    <body>
        <h2> Daily Data Breach Links | Health Care (USA Today)</h2>
        <p><strong>{len(breaches)} new healthcare data breaches reported</strong></p>
        
        <div class="info">
             <strong>Tip:</strong> Click any company name to search USA Today's database for that breach
        </div>
        
        <table class="breach-table">
            <thead>
                <tr>
                    <th>Company</th>
                    <th>Type</th>
                    <th>State</th>
                    <th>Date</th>
                    <th>People Affected</th>
                    <th>Breach Type</th>
                    <th>Source</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for breach in breaches:
        company = breach.get('company', '')
        company_type = breach.get('company_type', '')
        state = breach.get('state', '')
        breach_date = breach.get('breach_date', breach.get('date', ''))
        people = breach.get('people_affected', '')
        breach_type = breach.get('breach_type', '')
        breach_source = breach.get('breach_source', '')
        
        # Create search URL for company - when clicked, searches USA Today database
        search_query = company.replace(' ', '+').replace('&', '%26')
        search_url = f"https://data.usatoday.com/health-care-data-breaches/?search={search_query}"
        
        body += f"""
            <tr>
                <td>
                    <a href="{search_url}" target="_blank" class="breach-link" title="Search USA Today for {company}">
                         {company}
                    </a>
                </td>
                <td>{company_type}</td>
                <td>{state}</td>
                <td>{breach_date}</td>
                <td>{people}</td>
                <td>{breach_type}</td>
                <td>{breach_source}</td>
            </tr>
        """
    
    body += f"""
            </tbody>
        </table>
        
        <div class="footer">
            <p><em>Sent: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
            <p><em>Source: USA Today Healthcare Data Breaches Database</em></p>
            <p><em> Company names are clickable - click to view details on USA Today</em></p>
        </div>
    </body>
    </html>
    """
    
    return body


def main():
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║  Daily Data Breach Links | Health Care (USA Today)           ║
    ║                                                              ║
    ║  Scraping: USA Today Healthcare Breaches (10 pages)          ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    tracker = USATodayTracker()
    notifier = EmailNotifier()
    
    print("\n" + "="*70)
    print("RUNNING USA TODAY SCRAPER")
    print("="*70 + "\n")
    
    all_breaches = []
    
    for category, scrapers in USATODAY_REGISTRY.items():
        for scraper_key, scraper_info in scrapers.items():
            if not scraper_info.get('enabled', True):
                continue
            
            print(f"[{scraper_info['name']}] Starting...")
            
            try:
                import importlib
                module = importlib.import_module(scraper_info['module'])
                scraper_class = getattr(module, scraper_info['class'])
                scraper_instance = scraper_class()
                breaches = scraper_instance.run()
                print(f"[{scraper_info['name']}] Returned {len(breaches)} breaches")
                all_breaches.extend(breaches)
            except Exception as e:
                print(f"[{scraper_info['name']}] ERROR: {e}")
                import traceback
                traceback.print_exc()
    
    print("\n" + "="*70)
    print(f"TOTAL BREACHES SCRAPED: {len(all_breaches)}")
    print("="*70 + "\n")
    
    # Filter duplicates
    new_breaches = tracker.filter_new_urls(all_breaches)
    
    if not new_breaches:
        print("\n" + "="*70)
        print("NO NEW BREACHES - Sending notification")
        print("="*70 + "\n")
        
        no_breaches_body = f"""
        <html>
        <body>
            <h2> Daily Data Breach Links | Health Care (USA Today)</h2>
            <p><strong>No new healthcare data breaches found.</strong></p>
            <p>All breaches from today's scan have already been reported.</p>
            <hr>
            <p><em>Scanned: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
            <p><em>Total breaches checked: {len(all_breaches)}</em></p>
        </body>
        </html>
        """
        
        notifier.send_digest_email(
            subject="Daily Data Breach Links Health Care (USA Today) - No New Breaches",
            body=no_breaches_body,
            digest_data={},
            total_urls=0
        )
        print(" 'No new breaches' email sent\n")
    else:
        print("\n" + "="*70)
        print(f"SENDING EMAIL WITH {len(new_breaches)} NEW BREACHES")
        print("="*70 + "\n")
        
        email_body = create_table_email(new_breaches)
        
        notifier.send_digest_email(
            subject="Daily Data Breach Links Health Care (USA Today)",
            body=email_body,
            digest_data={'USA Today': new_breaches},
            total_urls=len(new_breaches)
        )
        
        print(" Email sent with clickable links\n")
        tracker.mark_as_sent(new_breaches)
    
    print("\n" + "="*70)
    print("EXECUTION COMPLETED")
    print("="*70)
    print(f"Total breaches scraped: {len(all_breaches)}")
    print(f"New breaches sent: {len(new_breaches)}")
    print(f"Duplicates filtered: {len(all_breaches) - len(new_breaches)}")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()