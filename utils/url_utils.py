#!/usr/bin/env python3
"""
URL Utility Functions
Helper functions for URL validation, normalization, and manipulation.
"""

import re
from urllib.parse import urlparse, urljoin, urlunparse


def normalize_url(url: str) -> str:
    """
    Normalize URL for consistent comparison.
    
    Args:
        url (str): URL to normalize
        
    Returns:
        str: Normalized URL
    """
    if not url:
        return ""
    
    try:
        parsed = urlparse(url)
        # Remove fragment, query params, and trailing slash
        normalized = urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path.rstrip('/'),
            '',  # params
            '',  # query
            ''   # fragment
        ))
        return normalized
    except Exception:
        return url.lower().rstrip('/')


def is_valid_url(url: str) -> bool:
    """
    Check if string is a valid URL.
    
    Args:
        url (str): URL to validate
        
    Returns:
        bool: True if valid URL
    """
    if not url or not isinstance(url, str):
        return False
    
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def get_domain(url: str) -> str:
    """
    Extract domain from URL.
    
    Args:
        url (str): URL
        
    Returns:
        str: Domain or empty string
    """
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower()
    except Exception:
        return ""


def make_absolute(url: str, base_url: str) -> str:
    """
    Convert relative URL to absolute.
    
    Args:
        url (str): URL (relative or absolute)
        base_url (str): Base URL for resolution
        
    Returns:
        str: Absolute URL
    """
    return urljoin(base_url, url)


def clean_url(url: str) -> str:
    """
    Clean URL by removing tracking parameters and fragments.
    
    Args:
        url (str): URL to clean
        
    Returns:
        str: Cleaned URL
    """
    if not url:
        return ""
    
    try:
        parsed = urlparse(url)
        # Keep only essential parts
        cleaned = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            '',  # params
            '',  # query (remove tracking)
            ''   # fragment
        ))
        return cleaned.rstrip('/')
    except Exception:
        return url