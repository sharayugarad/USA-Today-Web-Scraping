#!/usr/bin/env python3
"""
Logging Utility Functions
Helper functions for setting up and managing logging.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_scraper_logger(name: str, log_dir: Path, level=logging.INFO):
    """
    Setup logger for a scraper.
    
    Args:
        name (str): Logger name
        log_dir (Path): Directory for log files
        level: Logging level
        
    Returns:
        logging.Logger: Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # File handler
    log_file = log_dir / f"{name.lower().replace(' ', '_')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def log_scraper_start(logger, scraper_name: str):
    """Log scraper start with banner"""
    logger.info("="*60)
    logger.info(f"Starting {scraper_name} scraper")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)


def log_scraper_end(logger, scraper_name: str, count: int, duration: float):
    """Log scraper end with results"""
    logger.info("="*60)
    logger.info(f"{scraper_name} scraper completed")
    logger.info(f"Items scraped: {count}")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info("="*60)