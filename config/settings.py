#!/usr/bin/env python3
"""
Centralized Configuration for Mass ARB Scraper System
All settings and configuration in one place.
Updated with date filtering and last-run tracking.
"""

import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any

# ============================================================================
# PROJECT PATHS
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent.parent

# Configuration directories
CONFIG_DIR = BASE_DIR / 'config'
EMAIL_CONFIG_PATH = CONFIG_DIR / 'email_config.json'
SCRAPER_CONFIG_PATH = CONFIG_DIR / 'scraper_config.json'

# Path to the secret environment file containing email credentials
SECRET_ENV_PATH = CONFIG_DIR / 'secret.local.env'

# Load environment variables from secret.local.env
load_dotenv(dotenv_path=SECRET_ENV_PATH)

# Data directories
DATA_DIR = BASE_DIR / 'data'
SCRAPED_DIR = DATA_DIR / 'scraped'
PROGRESS_DIR = DATA_DIR / "progress"
LOGS_DIR = DATA_DIR / 'logs'
STORAGE_DIR = DATA_DIR / 'storage'

# Ensure all directories exist
SCRAPED_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

# Log file path
LOG_FILE = LOGS_DIR / 'mass_arb_scraper.log'

print(f"[✓] Data directories initialized")
print(f"    - Scraped data: {SCRAPED_DIR}")
print(f"    - Logs: {LOGS_DIR}")
print(f"    - Storage: {STORAGE_DIR}")


# Create directories if they don't exist
for directory in [DATA_DIR, SCRAPED_DIR, PROGRESS_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


# ============================================================================
# EMAIL CONFIGURATION
# ============================================================================

def load_email_config() -> Dict[str, Any]:
    """
    Load email configuration from secret.local.env (via environment variables).

    The credentials are stored in config/secret.local.env and loaded into
    environment variables by python-dotenv at the top of this module.
    The file path is: CONFIG_DIR / 'secret.local.env'
    (i.e., <project_root>/config/secret.local.env)

    Returns:
        dict: Email configuration with keys:
            smtp_server, smtp_port, use_ssl, sender_email,
            sender_password, receiver_emails
    """
    logger = logging.getLogger(__name__)

    if not SECRET_ENV_PATH.exists():
        logger.warning(f"Secret env file not found: {SECRET_ENV_PATH}")
        logger.warning("Please create config/secret.local.env with email credentials.")
        return {}

    # Read credentials from environment variables (populated from secret.local.env)
    mapped_config = {
        'smtp_server': os.getenv('SMTP_SERVER', 'smtp-mail.outlook.com'),
        'smtp_port': int(os.getenv('SMTP_PORT', '587')),
        'use_ssl': os.getenv('USE_SSL', 'false').lower() == 'true',
        'sender_email': os.getenv('SENDER_EMAIL', ''),
        'sender_password': os.getenv('SENDER_PASSWORD', ''),
    }

    # Parse receiver emails (comma-separated string -> list)
    receiver_emails_raw = os.getenv('RECEIVER_EMAILS', '')
    mapped_config['receiver_emails'] = [
        email.strip()
        for email in receiver_emails_raw.split(',')
        if email.strip()
    ]

    print(f"[OK] Email config loaded from: {SECRET_ENV_PATH}")
    print(f"     SMTP: {mapped_config['smtp_server']}:{mapped_config['smtp_port']}")
    print(f"     Sender: {mapped_config['sender_email']}")
    print(f"     Recipients: {mapped_config['receiver_emails']}")

    return mapped_config


# Load configurations
EMAIL_CONFIG = load_email_config()

# ============================================================================
# BATCH PROCESSING CONFIGURATION
# ============================================================================

BATCH_CONFIG = {
    'batch_size': int(os.getenv('BATCH_SIZE', '20')),
    'delay_minutes': int(os.getenv('BATCH_DELAY_MINUTES', '1')),
    'progress_file': PROGRESS_DIR / 'batch_progress.json',
    'last_run_file': PROGRESS_DIR / 'last_run.json'  # NEW: Track last run times
}


# ============================================================================
# DATE FILTERING CONFIGURATION (NEW)
# ============================================================================

DATE_FILTER_CONFIG = {
    # Only include content from last 24 hours (1 day)
    'hours_threshold': int(os.getenv('DATE_FILTER_HOURS', '24')),
    
    # Use last run time as reference (True) or fixed 24 hours (False)
    'use_last_run_time': os.getenv('USE_LAST_RUN_TIME', 'true').lower() == 'true',
    
    # Include items with no date found (fail-safe)
    'include_undated': os.getenv('INCLUDE_UNDATED', 'true').lower() == 'true',
    
    # Common date selectors for web scraping
    'date_selectors': [
        # Meta tags (most reliable)
        'meta[property="article:published_time"]',
        'meta[property="article:modified_time"]',
        'meta[name="publication_date"]',
        'meta[name="date"]',
        'meta[name="last-modified"]',
        
        # HTML5 time elements
        'time[datetime]',
        'time.published',
        'time.entry-date',
        'time.updated',
        'time.post-date',
        
        # Common class names
        '.published-date',
        '.post-date',
        '.entry-date',
        '.article-date',
        '.date-published',
        '.publish-date',
        '.filing-date',
        '.case-date',
        '.case-filed',
        '.updated-date',
    ]
}


# ============================================================================
# SCRAPER CONFIGURATION
# ============================================================================

SCRAPER_CONFIG = {
    'max_pages': int(os.getenv('MAX_PAGES', '100')),
    'request_delay': int(os.getenv('REQUEST_DELAY', '1')),
    'max_retries': int(os.getenv('MAX_RETRIES', '3')),
    'timeout': int(os.getenv('REQUEST_TIMEOUT', '30')),
    'user_agent': os.getenv('USER_AGENT', 
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
    
    # NEW: Date filtering enabled by default
    'enable_date_filtering': os.getenv('ENABLE_DATE_FILTERING', 'true').lower() == 'true',
}


# ============================================================================
# SCRAPER REGISTRY
# ============================================================================

SCRAPER_REGISTRY = {
    'law_firms': {
        'clarkson': {
            'name': 'Clarkson Law Firm',
            'url': 'https://clarksonlawfirm.com/',
            'module': 'scrapers.law_firms.clarkson',
            'class': 'ClarksonScraper',  # ← ADDED
            'enabled': True,
            'email_subject': 'Clarkson Law Firm Scrapped Link',
            'date_filtering': True
        },
        'crosner': {
            'name': 'Crosner Legal',
            'url': 'https://crosnerlegal.com/',
            'module': 'scrapers.law_firms.crosner',
            'class': 'CrosnerScraper',  # ← ADDED
            'enabled': True,
            'email_subject': 'Crosner Legal Scrapped Link',
            'date_filtering': True
        },
        'forthepeople': {
            'name': 'For The People',
            'url': 'https://www.forthepeople.com/',
            'module': 'scrapers.law_firms.forthepeople',
            'class': 'ForThePeopleScraper',  # ← ADDED
            'enabled': True,
            'email_subject': 'ForThePeople.com Scrapped Link',
            'date_filtering': True
        },
        'lantern_labaton': {
            'name': 'Lantern Labaton',
            'url': 'https://lantern.labaton.com',
            'module': 'scrapers.law_firms.lantern_labaton',
            'class': 'LanternLabatonScraper',  # ← ADDED
            'enabled': True,
            'email_subject': 'Lantern Labaton Scrapped Link',
            'date_filtering': True
        },
        'lowey': {
            'name': 'Lowey Dannenberg',
            'url': 'https://lowey.com',
            'module': 'scrapers.law_firms.lowey',
            'class': 'LoweyScraper',  # ← ADDED
            'enabled': True,
            'email_subject': 'Lowey Scrapped Link',
            'date_filtering': True
        },
        'shamis_gentile': {
            'name': 'Shamis & Gentile',
            'url': 'https://shamisgentile.com/',
            'module': 'scrapers.law_firms.shamis_gentile',
            'class': 'ShamisGentileScraper',  # ← ADDED
            'enabled': True,
            'email_subject': 'Shamis & Gentile Scrapped Link',
            'date_filtering': True
        },
        'berger_montague': {
            'name': 'Berger Montague',
            'url': 'https://investigations.bergermontague.com',
            'module': 'scrapers.law_firms.berger_montague',
            'class': 'BergerMontagueScraper',  # ← ADDED
            'enabled': True,
            'email_subject': 'Berger Montague Scrapped Link',
            'date_filtering': True
        },
        'siri_llp': {
            'name': 'Siri & Glimstad LLP',
            'url': 'https://www.sirillp.com',
            'module': 'scrapers.law_firms.siri_llp',
            'class': 'SiriLLPScraper',  # ← ADDED
            'enabled': True,
            'email_subject': 'Siri & Glimstad LLP Scrapped Link',
            'date_filtering': True
        },
        'toppe_firm': {
            'name': 'Toppefirm',
            'url': 'https://www.toppefirm.com',
            'module': 'scrapers.law_firms.toppe_firm',
            'class': 'ToppeFirmScraper',  
            'enabled': True,
            'email_subject': 'Toppefirm Scrapped Link',
            'date_filtering': True
        },
        'zr_claims': {
            'name': 'ZR Claims',
            'url': 'https://zrclaims.com',
            'module': 'scrapers.law_firms.zr_claims',
            'class': 'ZRClaimsScraper',  
            'enabled': True,
            'email_subject': 'ZR Claims Scrapped Link',
            'date_filtering': True
        },
        # 'brannlaw': {
        #     'name': 'Brann Law',
        #     'class': 'BrannLawScraper',
        #     'module': 'scrapers.law_firms.brannlaw',
        #     'enabled': True,
        #     'description': 'Brann & Isaacson client alerts'
        # },
    },
    'legal_resources': {
        'classaction_org': {
            'name': 'ClassAction.org',
            'url': 'https://www.classaction.org',
            'module': 'scrapers.legal_resources.classaction_org',
            'class': 'ClassActionOrgScraper',  
            'enabled': True,
            'email_subject': 'ClassAction Scrapped Link',
            'date_filtering': True
        },
        'consumer_protection': {
            'name': 'Consumer Protection Law',
            'url': 'https://consumersprotectionlaw.com',
            'module': 'scrapers.legal_resources.consumer_protection',
            'class': 'ConsumerProtectionScraper',  
            'enabled': True,
            'email_subject': 'ConsumerProtection Law Scrapped Link',
            'date_filtering': True
        },
    },
    'government': {
        'ny_ag': {
            'name': 'New York AG',
            'url': 'https://ag.ny.gov',
            'module': 'scrapers.government.ny_ag',
            'class': 'NYAGScraper',  # ← ADDED
            'enabled': True,
            'email_subject': 'New York AG Scrapped Link',
            'date_filtering': True
        },
        'tx_ag': {
            'name': 'Texas AG',
            'url': 'https://www.texasattorneygeneral.gov',
            'module': 'scrapers.government.tx_ag',
            'class': 'TXAGScraper',  # ← ADDED
            'enabled': True,
            'email_subject': 'Texas AG Scrapped Link',
            'date_filtering': True
        }
    }
}

# Generic links scrapers (for separate email)
GENERIC_SCRAPERS_REGISTRY = {
    'law_firms': {
        'brannlaw': {
            'name': 'Brann Law',
            'class': 'BrannLawScraper',
            'module': 'scrapers.law_firms.brannlaw',
            'enabled': True,
            'description': 'Brann & Isaacson client alerts'
        },
    },
    'legal_resources': {
        'joinclassactions': {
            'name': 'Join Class Actions',
            'class': 'JoinClassActionsScraper',
            'module': 'scrapers.legal_resources.joinclassactions',
            'enabled': True,
            'description': 'Class action lawsuits database'
        },
    }
}

# Breachsense Registry (separate email)
BREACHSENSE_REGISTRY = {
    'legal_resources': {
        'breachsense': {
            'name': 'Breachsense',
            'class': 'BreachsenseScraper',
            'module': 'scrapers.legal_resources.breachsense',
            'enabled': True,
            'description': 'Data breach monitoring'
        }
    }
}

# USA Today Registry (separate email)
USATODAY_REGISTRY = {
    'legal_resources': {
        'usatoday_breaches': {
            'name': 'USA Today Healthcare Breaches',
            'class': 'USATodayBreachesScraper',
            'module': 'scrapers.legal_resources.usatoday_breaches',
            'enabled': True,
            'description': 'Healthcare data breaches'
        }
    }
}

# =============================================================================
# NEW BREACH MONITORING REGISTRY
# =============================================================================

# NEW_BREACH_MONITORING_REGISTRY = {
#     'breach_monitoring': {
#         'dexpose_io': {
#             'name': 'DeXpose.io',
#             'class': 'DexposeIOScraper',
#             'module': 'scrapers.breach_monitoring.dexpose_io',
#             'enabled': True,
#             'description': 'Ransomware and breach intelligence'
#         },
#         'slfla_com': {
#             'name': 'SLFLA.com',
#             'class': 'SLFLAScraper',
#             'module': 'scrapers.breach_monitoring.slfla_com',
#             'enabled': True,
#             'description': 'Legal class action news'
#         },
#         'redpacketsecurity_com': {
#             'name': 'RedPacketSecurity',
#             'class': 'RedPacketSecurityScraper',
#             'module': 'scrapers.breach_monitoring.redpacketsecurity_com',
#             'enabled': True,
#             'description': 'Cybersecurity breach news'
#         },
#         'botcrawl_com': {
#             'name': 'BotCrawl.com',
#             'class': 'BotCrawlScraper',
#             'module': 'scrapers.breach_monitoring.botcrawl_com',
#             'enabled': True,
#             'description': 'Bot and breach intelligence'
#         }
#     }
# }


# =============================================================================
# Ransomware Links Registry (separate email)
# =============================================================================

RANSOMWARE_REGISTRY = {
    'breach_monitoring': {
        'dexpose_io': {
            'name': 'DeXpose.io',
            'class': 'DexposeIOScraper',
            'module': 'scrapers.legal_resources.breach_monitoring.dexpose_io',
            'enabled': True,
            'description': 'Ransomware and breach intelligence'
        },
        'redpacketsecurity_com': {
            'name': 'RedPacketSecurity',
            'class': 'RedPacketSecurityScraper',
            'module': 'scrapers.legal_resources.breach_monitoring.redpacketsecurity_com',
            'enabled': True,
            'description': 'Cybersecurity breach news'
        },
        'botcrawl_com': {
            'name': 'BotCrawl.com',
            'class': 'BotCrawlScraper',
            'module': 'scrapers.legal_resources.breach_monitoring.botcrawl_com',
            'enabled': True,
            'description': 'Bot and breach intelligence'
        },
        'hookphish_com': {
            'name': 'HookPhish.com',
            'class': 'HookPhishScraper',
            'module': 'scrapers.legal_resources.breach_monitoring.hookphish_com',
            'enabled': True,
            'description': 'Phishing and security news'
        },
        'databreach_io': {
            'name': 'DataBreach.io',
            'class': 'DataBreachIOScraper',
            'module': 'scrapers.legal_resources.breach_monitoring.databreach_io',
            'enabled': True,
            'description': 'Data breach intelligence'
        },
        'rankiteo': {
            'name': 'Rankiteo Blog',
            'class': 'RankiteoScraper',
            'module': 'scrapers.legal_resources.rankiteo',
            'enabled': True,
            'description': 'Security and breach blog'
        },
        'slfla_com': {
            'name': 'SLFLA.com',
            'class': 'SLFLAScraper',
            'module': 'scrapers.breach_monitoring.slfla_com',
            'enabled': True,
            'description': 'Legal class action news'
        }
    }
}


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.FileHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'filename': str(LOGS_DIR / 'mass_arb_scraper.log'),
            'mode': 'a',
            'encoding': 'utf-8'
        }
    },
    'loggers': {
        '': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': True
        }
    }
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_scraper_info(scraper_key):
    """
    Get scraper information by key.
    
    Args:
        scraper_key (str): Scraper key (e.g., 'clarkson', 'ny_ag')
        
    Returns:
        dict: Scraper configuration or None if not found
    """
    # for category, scrapers in SCRAPER_REGISTRY.items():
    for category, scrapers in SCRAPER_REGISTRY.items():
        if scraper_key in scrapers:
            return scrapers[scraper_key]
    return None


def get_all_enabled_scrapers():
    """
    Get all enabled scrapers.
    
    Returns:
        list: List of tuples (category, scraper_key, scraper_info)
    """
    enabled = []
    # for category, scrapers in SCRAPER_REGISTRY.items():
    for category, scrapers in SCRAPER_REGISTRY.items():
        for key, info in scrapers.items():
            if info.get('enabled', True):
                enabled.append((category, key, info))
    return enabled


def is_date_filtering_enabled(scraper_key):
    """
    Check if date filtering is enabled for a specific scraper.
    
    Args:
        scraper_key (str): Scraper key
        
    Returns:
        bool: True if date filtering is enabled
    """
    scraper_info = get_scraper_info(scraper_key)
    if not scraper_info:
        return False
    
    # Check scraper-specific setting first
    if 'date_filtering' in scraper_info:
        return scraper_info['date_filtering']
    
    # Fall back to global setting
    return SCRAPER_CONFIG.get('enable_date_filtering', True)


def print_config_summary():
    """Print configuration summary for debugging."""
    print("\n" + "="*60)
    print("MASS ARB SCRAPER - CONFIGURATION SUMMARY")
    print("="*60)
    
    print("\n📁 PATHS:")
    print(f"  Base Directory: {BASE_DIR}")
    print(f"  Data Directory: {DATA_DIR}")
    print(f"  Scraped Data: {SCRAPED_DIR}")
    print(f"  Progress Files: {PROGRESS_DIR}")
    print(f"  Logs: {LOGS_DIR}")
    
    print("\n📧 EMAIL:")
    print(f"  SMTP Server: {EMAIL_CONFIG['smtp_server']}:{EMAIL_CONFIG['smtp_port']}")
    print(f"  Sender: {EMAIL_CONFIG['sender_email']}")
    print(f"  Recipients: {', '.join(EMAIL_CONFIG['receiver_emails'])}")
    print(f"  SSL: {EMAIL_CONFIG['use_ssl']}")
    
    print("\n📦 BATCH PROCESSING:")
    print(f"  Batch Size: {BATCH_CONFIG['batch_size']} URLs")
    print(f"  Delay: {BATCH_CONFIG['delay_minutes']} minutes")
    print(f"  Last Run File: {BATCH_CONFIG['last_run_file']}")
    
    print("\n📅 DATE FILTERING: (NEW)")
    print(f"  Hours Threshold: {DATE_FILTER_CONFIG['hours_threshold']} hours")
    print(f"  Use Last Run Time: {DATE_FILTER_CONFIG['use_last_run_time']}")
    print(f"  Include Undated: {DATE_FILTER_CONFIG['include_undated']}")
    
    print("\n🔧 SCRAPER SETTINGS:")
    print(f"  Max Pages: {SCRAPER_CONFIG['max_pages']}")
    print(f"  Request Delay: {SCRAPER_CONFIG['request_delay']}s")
    print(f"  Max Retries: {SCRAPER_CONFIG['max_retries']}")
    print(f"  Timeout: {SCRAPER_CONFIG['timeout']}s")
    print(f"  Date Filtering: {SCRAPER_CONFIG['enable_date_filtering']}")
    
    print("\n🌐 REGISTERED SCRAPERS:")
    total = 0
    enabled = 0
    date_filtered = 0
    # for category, scrapers in SCRAPER_REGISTRY.items():
    for category, scrapers in SCRAPER_REGISTRY.items():
        print(f"\n  {category.upper()}:")
        for key, info in scrapers.items():
            status = "✓" if info.get('enabled', True) else "✗"
            date_filter = "📅" if info.get('date_filtering', False) else "  "
            print(f"    {status} {date_filter} {key}: {info['name']}")
            total += 1
            if info.get('enabled', True):
                enabled += 1
            if info.get('date_filtering', False):
                date_filtered += 1
    
    print(f"\n  Total: {total} scrapers")
    print(f"  Enabled: {enabled} scrapers")
    print(f"  With Date Filtering: {date_filtered} scrapers")
    print("="*60 + "\n")


# ============================================================================
# VALIDATION
# ============================================================================

def validate_config():
    """Validate configuration and show warnings."""
    warnings = []
    
    # Check email config
    if not EMAIL_CONFIG['sender_email']:
        warnings.append("❌ EMAIL_USERNAME not configured")
    if not EMAIL_CONFIG['sender_password']:
        warnings.append("❌ EMAIL_PASSWORD not configured")
    if not EMAIL_CONFIG['receiver_emails']:
        warnings.append("❌ RECEIVER_EMAIL not configured")
    
    # Check directories
    for name, path in [
        ("Data", DATA_DIR),
        ("Scraped", SCRAPED_DIR),
        ("Progress", PROGRESS_DIR),
        ("Logs", LOGS_DIR)
    ]:
        if not path.exists():
            warnings.append(f"❌ {name} directory does not exist: {path}")
    
    if warnings:
        print("\n" + "="*60)
        print("⚠️  CONFIGURATION WARNINGS")
        print("="*60)
        for warning in warnings:
            print(f"  {warning}")
        print("\n💡 To fix:")
        print("  1. Create a .env file with required variables")
        print("  2. Or update config/email_config.json")
        print("  3. Run: python -m config.settings (to test)")
        print("="*60 + "\n")
        return False
    
    print("✅ Configuration validated successfully!\n")
    return True


# ============================================================================
# MODULE TESTING
# ============================================================================

if __name__ == "__main__":
    # Test configuration
    print_config_summary()
    validate_config()
