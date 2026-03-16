# USA Today Healthcare Data Breach Scraper System - Technical Documentation

## 1. Project Overview

This is an **automated web scraping and email notification system** that monitors healthcare data breaches reported on the USA Today public database. The system scrapes breach records from the USA Today website, deduplicates them against previously sent records, and emails a formatted HTML report to a team of recipients.

The system is part of a larger **Mass Arbitration (ARB) Scraper Platform** that monitors multiple legal, government, and breach-related websites. This document focuses on the USA Today Healthcare Breaches pipeline, which operates as an independent module within the platform.

---

## 2. Architecture Overview

```
+--------------------+       +--------------------+       +--------------------+
|   Entry Point      |       |   Scraper Layer    |       |   Notification     |
|                    |       |                    |       |   Layer            |
| main_usatoday_     | ----> | USATodayBreaches   | ----> | EmailNotifier      |
| breaches.py        |       | Scraper            |       | (core/notifier.py) |
|                    |       | (Selenium-based)   |       |                    |
+--------------------+       +--------------------+       +--------------------+
         |                           |                            |
         v                           v                            v
+--------------------+       +--------------------+       +--------------------+
|   Configuration    |       |   Data Storage     |       |   Email Delivery   |
|                    |       |                    |       |                    |
| config/settings.py |       | data/scraped/      |       | SMTP via Outlook/  |
| config/secret.     |       | (JSON files)       |       | Gmail              |
| local.env          |       |                    |       |                    |
+--------------------+       +--------------------+       +--------------------+
```

### Data Flow (USA Today Pipeline)

1. `main_usatoday_breaches.py` is executed (manually or via cron)
2. The scraper registry in `config/settings.py` identifies which scraper to run
3. `USATodayBreachesScraper` launches a headless Chrome browser via Selenium
4. The scraper navigates to the USA Today healthcare breaches database
5. It paginates through up to 10 pages, extracting table rows
6. Each breach record is hashed (MD5) for deduplication
7. Records are compared against `data/scraped/usatoday_sent_urls.json` (previously sent)
8. New (unseen) breaches are formatted into an HTML email with clickable company links
9. The email is sent via SMTP to configured recipients
10. Sent breach hashes are saved to the tracking file for future deduplication

---

## 3. Project Structure

```
USA-Today-Web-Scraping/
|
|-- main_usatoday_breaches.py        # Entry point for USA Today breach scraping
|
|-- config/
|   |-- __init__.py
|   |-- settings.py                  # Centralized configuration (paths, registries, helpers)
|   |-- secret.local.env             # Email credentials (NOT committed to git)
|   |-- email_config.json            # Reference-only config (credentials moved to .env)
|
|-- core/
|   |-- __init__.py
|   |-- orchestrator.py              # Digest-mode orchestrator for all scrapers
|   |-- orchestrator_og.py           # Original orchestrator (per-scraper email mode)
|   |-- batch_processor.py           # Batch URL processing, deduplication, progress tracking
|   |-- notifier.py                  # Email notification via SMTP
|
|-- scrapers/
|   |-- __init__.py
|   |-- base_scraper.py              # Abstract base class for HTTP-based scrapers
|   |-- legal_resources/
|       |-- __init__.py
|       |-- usatoday_breaches.py     # Selenium-based USA Today breach scraper
|
|-- utils/
|   |-- __init__.py                  # Re-exports date utility functions
|   |-- date_utils.py               # Date parsing, comparison, formatting
|   |-- url_utils.py                # URL validation, normalization, cleaning
|   |-- file_utils.py               # JSON read/write helpers
|   |-- logging_utils.py            # Logger setup helpers
|
|-- scripts/
|   |-- __init__.py
|   |-- run_single_scraper.py        # CLI: run one scraper by name
|   |-- run_all_scrapers.py          # CLI: run all enabled scrapers
|   |-- list_scrapers.py             # CLI: list available scrapers
|   |-- reset_progress.py            # CLI: reset deduplication tracking
|   |-- test_email.py               # CLI: test email configuration
|
|-- data/
|   |-- scraped/                     # Scraped JSON data files + tracking files
|   |-- progress/                    # Batch progress and last-run tracking
|   |-- logs/                        # Per-scraper log files
|   |-- output/                      # Final exported data (JSON, Excel)
|   |-- storage/                     # General storage
|
|-- requirements.txt                 # Python dependencies (pip-compiled)
|-- requirements.in                  # Source dependency list
|-- .gitignore                       # Git ignore rules (excludes secrets, venvs, etc.)
```

---

## 4. Module-by-Module Documentation

### 4.1 `main_usatoday_breaches.py` (Entry Point)

**Purpose:** Orchestrates the USA Today healthcare breach scraping pipeline end-to-end.

**Key Classes:**
- `USATodayTracker` - Manages deduplication by tracking previously sent breach hashes in `data/scraped/usatoday_sent_urls.json`.

**Key Functions:**
- `create_table_email(breaches)` - Generates a styled HTML email body with a table of breaches. Each company name is a clickable link that searches the USA Today database for that company.
- `main()` - Entry point that:
  1. Initializes the tracker and email notifier
  2. Iterates through scrapers in the `USATODAY_REGISTRY`
  3. Dynamically imports and runs each scraper
  4. Filters out previously sent breaches
  5. Sends an HTML email (either with breach data or a "no new breaches" notification)
  6. Updates the tracking file

**How to run:**
```bash
python main_usatoday_breaches.py
```

---

### 4.2 `config/settings.py` (Configuration Hub)

**Purpose:** Single source of truth for all configuration: paths, email settings, scraper registries, batch processing, date filtering, and logging.

**Key Constants:**
| Constant | Description |
|----------|-------------|
| `BASE_DIR` | Project root directory |
| `SECRET_ENV_PATH` | Path to `config/secret.local.env` (credential file) |
| `SCRAPED_DIR` | Directory for scraped JSON data |
| `PROGRESS_DIR` | Directory for batch progress/last-run files |
| `LOGS_DIR` | Directory for per-scraper log files |
| `EMAIL_CONFIG` | Dict loaded from `secret.local.env` at module init |
| `BATCH_CONFIG` | Batch size (20), delay (1 min), progress file paths |
| `DATE_FILTER_CONFIG` | 24-hour threshold, date CSS selectors |
| `SCRAPER_CONFIG` | Max pages (100), request delay (1s), retries (3), timeout (30s) |

**Scraper Registries:**
| Registry | Purpose |
|----------|---------|
| `SCRAPER_REGISTRY` | 14 primary scrapers (law firms, legal resources, government) |
| `GENERIC_SCRAPERS_REGISTRY` | Generic link scrapers (Brann Law, Join Class Actions) |
| `BREACHSENSE_REGISTRY` | Breachsense data breach monitoring |
| `USATODAY_REGISTRY` | USA Today Healthcare Breaches (used by `main_usatoday_breaches.py`) |
| `RANSOMWARE_REGISTRY` | Ransomware/security breach monitoring scrapers |

**Email Credential Loading:**
Email credentials are fetched from `config/secret.local.env` using `python-dotenv`. The file path is defined in the code as:
```python
SECRET_ENV_PATH = CONFIG_DIR / 'secret.local.env'
```
The `load_email_config()` function reads the following environment variables from that file:
- `SMTP_SERVER` - SMTP host (default: smtp-mail.outlook.com)
- `SMTP_PORT` - SMTP port (default: 587)
- `USE_SSL` - Whether to use SSL (default: false)
- `SENDER_EMAIL` - Sender email address
- `SENDER_PASSWORD` - Sender app password
- `RECEIVER_EMAILS` - Comma-separated list of recipient addresses

---

### 4.3 `config/secret.local.env` (Credentials File)

**Purpose:** Stores sensitive email credentials separately from the codebase. This file is listed in `.gitignore` and must never be committed to version control.

**Location:** `config/secret.local.env`

**Format:**
```env
SMTP_SERVER=smtp-mail.outlook.com
SMTP_PORT=587
USE_SSL=false
SENDER_EMAIL=your-email@gmail.com
SENDER_PASSWORD=your-app-password
RECEIVER_EMAILS=recipient1@example.com, recipient2@example.com
```

**How it works:** At startup, `config/settings.py` calls `load_dotenv(dotenv_path=SECRET_ENV_PATH)` which reads this file and populates `os.environ` with the key-value pairs. The `load_email_config()` function then reads these values with `os.getenv()`.

---

### 4.4 `core/notifier.py` (Email Notification)

**Purpose:** Sends HTML-formatted emails via SMTP with TLS encryption.

**Class: `EmailNotifier`**
- Reads config from `EMAIL_CONFIG` (loaded from `secret.local.env`)
- `send_digest_email(subject, body, digest_data, total_urls)` - Sends an HTML email to all configured recipients
- Uses `smtplib.SMTP` with STARTTLS for secure transmission
- Includes email validation (`_is_valid_email`) and config completeness checking (`_is_config_complete`)

**Standalone test:** `test_email_config()` sends a test email to verify the configuration works.

---

### 4.5 `core/orchestrator.py` (Digest-Mode Orchestrator)

**Purpose:** Runs multiple scrapers and sends a single consolidated daily digest email with all results combined.

**Class: `ScraperOrchestrator`**
- Accepts optional `scraper_registry` parameter to run different sets of scrapers
- Accepts optional `email_subject` to customize the email subject line
- `run_all_scrapers()` - Iterates through all enabled scrapers, collects URLs, then sends one digest email
- `_execute_scraper()` - Dynamically imports and runs a scraper class, collecting results
- `_build_digest_email_body()` - Generates HTML email with sections grouped by scraper source
- `send_daily_digest()` - Sends the consolidated email and updates last-run times for all scrapers
- Deduplication is performed via the `BatchProcessor` before adding URLs to the digest

---

### 4.6 `core/orchestrator_og.py` (Original Per-Scraper Orchestrator)

**Purpose:** The original orchestrator that sends separate emails per scraper (non-digest mode). Used by the CLI scripts in `scripts/`.

**Class: `ScraperOrchestrator`**
- `run_all_scrapers()` - Runs all enabled scrapers sequentially
- `run_specific_scraper(scraper_name)` - Runs a single scraper by name or key
- `run_category(category)` - Runs all scrapers in a category (law_firms, legal_resources, government)
- Each scraper's URLs are processed immediately through `BatchProcessor.process_batch()`

---

### 4.7 `core/batch_processor.py` (Batch Processing & Deduplication)

**Purpose:** Handles URL deduplication, batch email sending, progress tracking, and last-run time management.

**Class: `BatchProcessor`**
- **Deduplication:** Loads all existing URLs from `data/scraped/*.json` + `data/progress/batch_progress.json` at startup. Uses both direct URL comparison and MD5 hash comparison.
- **Batch sending:** Splits URLs into configurable batches (default: 20) with delays between batches (default: 1 minute)
- **Progress tracking:** Saves sent URLs to `data/progress/batch_progress.json`
- **Last-run tracking:** Saves per-scraper timestamps to `data/progress/last_run.json` for date-based filtering

**Key methods:**
| Method | Description |
|--------|-------------|
| `is_duplicate(url)` | Check if URL was already scraped |
| `filter_unique_urls(links)` | Remove duplicates from a batch |
| `process_batch(urls, name, key)` | Process and email URLs in batches |
| `get_last_run_time(key)` | Get when a scraper last ran |
| `update_last_run_time(key, name, count, success)` | Record a scraper's run |

---

### 4.8 `scrapers/base_scraper.py` (Abstract Base Scraper)

**Purpose:** Provides a common foundation for all HTTP-based scrapers (requests + BeautifulSoup).

**Class: `BaseScraper(ABC)`**
- Manages an HTTP session with User-Agent, retries, and timeouts
- Provides `fetch_page(url)` with exponential backoff retry logic
- Provides date extraction from HTML pages using configurable CSS selectors
- Provides URL normalization, domain checking, and deduplication
- Provides data persistence (saves scraped data to JSON)
- Subclasses must implement `is_valid_url(url)` and `scrape()`

**Note:** The USA Today scraper does NOT inherit from `BaseScraper` because it uses Selenium (browser automation) instead of HTTP requests. It has its own standalone implementation.

---

### 4.9 `scrapers/legal_resources/usatoday_breaches.py` (USA Today Scraper)

**Purpose:** Scrapes the USA Today Healthcare Data Breaches database using Selenium WebDriver.

**Class: `USATodayBreachesScraper`**
- **Browser:** Headless Chrome via Selenium
- **Target URL:** `https://data.usatoday.com/health-care-data-breaches/`
- **Pagination:** Navigates up to 10 pages using CSS selector and text-based button detection
- **Data extracted per row:** Company, State, Company Type, Breach Date, People Affected, Breach Type, Breach Source
- **Deduplication:** Creates MD5 hash from `company|state|date|people_affected` to detect cross-page duplicates
- **Date filtering:** Only includes breaches from the last 30 days
- `run()` method returns a list of dicts compatible with the orchestrator

---

### 4.10 `utils/` (Utility Modules)

**`date_utils.py`** - Flexible date parsing:
- `DateParser` class handles ISO 8601, US/EU formats, and relative dates ("2 hours ago", "yesterday")
- `is_within_last_24_hours(date)` - Recency check
- `is_newer_than(date, reference)` - Comparison check
- `extract_date_from_html(soup, selectors)` - Extract dates from HTML using CSS selectors

**`url_utils.py`** - URL manipulation:
- `normalize_url()`, `is_valid_url()`, `get_domain()`, `make_absolute()`, `clean_url()`

**`file_utils.py`** - File I/O helpers:
- `save_json()`, `load_json()`, `append_to_jsonl()`, `read_jsonl()`, `ensure_dir()`

**`logging_utils.py`** - Logger setup:
- `setup_scraper_logger()` - Creates file + console logger for a scraper

---

### 4.11 `scripts/` (CLI Utilities)

| Script | Command | Description |
|--------|---------|-------------|
| `run_all_scrapers.py` | `python scripts/run_all_scrapers.py` | Run all enabled scrapers |
| `run_single_scraper.py` | `python scripts/run_single_scraper.py clarkson` | Run a specific scraper by key |
| `list_scrapers.py` | `python scripts/list_scrapers.py` | Show all registered scrapers |
| `test_email.py` | `python scripts/test_email.py` | Send a test email to verify config |
| `reset_progress.py` | `python scripts/reset_progress.py` | Reset all deduplication tracking |

---

## 5. Email Credential Management

### How Credentials Are Loaded

Credentials are stored in `config/secret.local.env` and loaded securely:

```
config/secret.local.env  -->  python-dotenv  -->  os.environ  -->  load_email_config()  -->  EMAIL_CONFIG dict
```

**Step-by-step:**
1. `config/settings.py` defines `SECRET_ENV_PATH = CONFIG_DIR / 'secret.local.env'`
2. `load_dotenv(dotenv_path=SECRET_ENV_PATH)` reads the file and sets environment variables
3. `load_email_config()` reads these env vars with `os.getenv()`
4. The resulting `EMAIL_CONFIG` dict is used by `core/notifier.py`

### Security

- `secret.local.env` is listed in `.gitignore` - it will never be committed
- `email_config.json` no longer contains actual credentials (reference-only)
- No credentials are hardcoded in Python source files

---

## 6. Key Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `selenium` | 4.39.0 | Browser automation for USA Today scraping |
| `beautifulsoup4` | 4.14.2 | HTML parsing for HTTP-based scrapers |
| `requests` | 2.32.5 | HTTP client for web requests |
| `lxml` | 6.0.2 | Fast HTML/XML parser (BeautifulSoup backend) |
| `python-dotenv` | 1.2.1 | Load environment variables from `.env` files |
| `python-dateutil` | 2.9.0 | Advanced date parsing |
| `webdriver-manager` | 4.0.2 | Auto-manage ChromeDriver binaries |

---

## 7. Execution Flow Diagram

```
[Start: main_usatoday_breaches.py]
           |
           v
  Load USATODAY_REGISTRY from config/settings.py
  Load EMAIL_CONFIG from config/secret.local.env
           |
           v
  Initialize USATodayTracker (load previously sent hashes)
  Initialize EmailNotifier (SMTP config from secret.local.env)
           |
           v
  For each scraper in USATODAY_REGISTRY:
      |
      v
  Dynamically import USATodayBreachesScraper
  Launch headless Chrome -> Navigate to USA Today
      |
      v
  For each page (1 to 10):
      Extract table rows -> Parse fields
      Create MD5 hash per breach -> Skip duplicates
      Filter by date (last 30 days)
      |
      v
  Return list of unique breach dicts
           |
           v
  Filter against USATodayTracker (remove previously sent)
           |
           +--- No new breaches? --> Send "no new breaches" email
           |
           +--- New breaches found? --> Generate HTML table email
                                        with clickable company links
                                        --> Send via SMTP
                                        --> Update tracking file
           |
           v
       [End: Print summary]
```

---

## 8. Data Format

Each breach record is a dictionary with these fields:

```json
{
  "title": "Company Name - State - N people affected",
  "url": "https://data.usatoday.com/health-care-data-breaches/#<hash>",
  "description": "Company Type: ... | State: ... | Breach Date: ... | ...",
  "date": "2025-01-15",
  "source": "USA Today Healthcare Breaches",
  "breach_hash": "md5_hash_string",
  "company": "Company Name",
  "state": "CA",
  "company_type": "Healthcare Provider",
  "breach_date": "Jan. 15, 2025",
  "people_affected": "50,000",
  "breach_type": "Hacking/IT Incident",
  "breach_source": "Network Server"
}
```

---

## 9. How to Set Up and Run

### Prerequisites
- Python 3.12+
- Google Chrome browser installed
- ChromeDriver (auto-managed by `webdriver-manager`)

### Installation

```bash
# 1. Clone the repository
git clone <repo-url>
cd USA-Today-Web-Scraping

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure email credentials
# Edit config/secret.local.env with your SMTP credentials:
#   SMTP_SERVER, SMTP_PORT, SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAILS

# 5. Test email configuration
python scripts/test_email.py

# 6. Run the USA Today breach scraper
python main_usatoday_breaches.py
```

### Scheduling (Cron Example)

```bash
# Run daily at 8:00 AM
0 8 * * * cd /path/to/USA-Today-Web-Scraping && /path/to/venv/bin/python main_usatoday_breaches.py >> /tmp/usatoday_cron.log 2>&1
```

---

## 10. Summary

This system automates the monitoring of USA Today's healthcare data breach database. It uses Selenium to scrape breach records, deduplicates them against historical data, and delivers formatted HTML email reports to stakeholders. Email credentials are securely managed through a separate `config/secret.local.env` file that is excluded from version control. The modular architecture (configuration, scraping, notification, utilities) allows the system to be extended with additional data sources.
