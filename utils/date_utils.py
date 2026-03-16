#!/usr/bin/env python3
"""
Date Parsing and Comparison Utilities
Handles flexible date parsing from various formats and recency checks.
"""

from datetime import datetime, timedelta, timezone
import re
from typing import Optional, Union
import logging

logger = logging.getLogger(__name__)


class DateParser:
    """
    Flexible date parser that handles multiple date formats.
    """
    
    # Common date format patterns
    DATE_PATTERNS = [
        # ISO formats
        (r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[Z+-]\d{0,5})', '%Y-%m-%dT%H:%M:%S%z'),  # ISO 8601 with timezone
        (r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', '%Y-%m-%dT%H:%M:%S'),  # ISO 8601 without timezone
        (r'(\d{4}-\d{2}-\d{2})', '%Y-%m-%d'),  # 2024-11-03
        
        # US formats
        (r'(\w+ \d{1,2}, \d{4})', '%B %d, %Y'),  # November 3, 2024
        (r'(\w+ \d{1,2} \d{4})', '%B %d %Y'),    # November 3 2024
        (r'(\d{1,2}/\d{1,2}/\d{4})', '%m/%d/%Y'),  # 11/03/2024
        
        # EU formats
        (r'(\d{1,2}-\d{1,2}-\d{4})', '%d-%m-%Y'),  # 03-11-2024
        (r'(\d{1,2}\.\d{1,2}\.\d{4})', '%d.%m.%Y'),  # 03.11.2024
        
        # Abbreviated formats
        (r'(\w{3} \d{1,2}, \d{4})', '%b %d, %Y'),  # Nov 3, 2024
        (r'(\d{1,2} \w+ \d{4})', '%d %B %Y'),      # 3 November 2024
    ]
    
    @staticmethod
    def parse(date_input: Union[str, datetime, None]) -> Optional[datetime]:
        """
        Parse date from various input formats.
        
        Args:
            date_input: String, datetime object, or None
            
        Returns:
            datetime object or None if parsing fails
        """
        if not date_input:
            return None
        
        # Already a datetime object
        if isinstance(date_input, datetime):
            return date_input
        
        # Convert to string
        date_string = str(date_input).strip()
        
        if not date_string:
            return None
        
        # Try each pattern
        for pattern, date_format in DateParser.DATE_PATTERNS:
            match = re.search(pattern, date_string)
            if match:
                try:
                    date_str = match.group(1)
                    parsed = datetime.strptime(date_str, date_format)
                    
                    # If no timezone info, assume UTC
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    
                    return parsed
                except ValueError:
                    continue
        
        # Try relative dates (e.g., "2 hours ago", "yesterday")
        relative_date = DateParser._parse_relative_date(date_string)
        if relative_date:
            return relative_date
        
        logger.warning(f"Could not parse date: {date_string}")
        return None
    
    @staticmethod
    def _parse_relative_date(text: str) -> Optional[datetime]:
        """
        Parse relative dates like "2 hours ago", "yesterday".
        
        Args:
            text: Relative date string
            
        Returns:
            datetime object or None
        """
        text_lower = text.lower()
        now = datetime.now(timezone.utc)
        
        # Today
        if 'today' in text_lower or 'just now' in text_lower:
            return now
        
        # Yesterday
        if 'yesterday' in text_lower:
            return now - timedelta(days=1)
        
        # X hours ago
        hours_match = re.search(r'(\d+)\s*hours?\s*ago', text_lower)
        if hours_match:
            hours = int(hours_match.group(1))
            return now - timedelta(hours=hours)
        
        # X minutes ago
        minutes_match = re.search(r'(\d+)\s*minutes?\s*ago', text_lower)
        if minutes_match:
            minutes = int(minutes_match.group(1))
            return now - timedelta(minutes=minutes)
        
        # X days ago
        days_match = re.search(r'(\d+)\s*days?\s*ago', text_lower)
        if days_match:
            days = int(days_match.group(1))
            return now - timedelta(days=days)
        
        return None


def is_within_last_24_hours(date: Optional[datetime]) -> bool:
    """
    Check if date is within the last 24 hours.
    
    Args:
        date: datetime object or None
        
    Returns:
        bool: True if within last 24 hours, True if date is None (fail-safe)
    """
    if not date:
        # If no date found, include it (better than missing content)
        return True
    
    # Ensure both dates have timezone info for comparison
    now = datetime.now(timezone.utc)
    
    if date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)
    
    # Calculate time difference
    delta = now - date
    
    # Check if within 24 hours
    return delta.total_seconds() <= 86400  # 24 hours = 86400 seconds


def is_newer_than(date: Optional[datetime], reference_date: Optional[datetime]) -> bool:
    """
    Check if date is newer than reference date.
    
    Args:
        date: Date to check
        reference_date: Reference date to compare against
        
    Returns:
        bool: True if date is newer, True if date is None (fail-safe)
    """
    if not date:
        # No date found - include it (fail-safe)
        return True
    
    if not reference_date:
        # No reference date - include everything
        return True
    
    # Ensure both have timezone info
    if date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)
    
    if reference_date.tzinfo is None:
        reference_date = reference_date.replace(tzinfo=timezone.utc)
    
    return date > reference_date


def extract_date_from_html(soup, selectors: list) -> Optional[datetime]:
    """
    Extract date from HTML using multiple selectors.
    
    Args:
        soup: BeautifulSoup object
        selectors: List of CSS selectors to try
        
    Returns:
        datetime object or None
    """
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                # Try datetime attribute first
                date_str = element.get('datetime') or element.get('content')
                
                # If no attribute, try text content
                if not date_str:
                    date_str = element.get_text(strip=True)
                
                # Parse the date
                parsed = DateParser.parse(date_str)
                if parsed:
                    return parsed
        except Exception as e:
            logger.debug(f"Error extracting date with selector '{selector}': {e}")
            continue
    
    return None


def get_common_date_selectors() -> list:
    """
    Get list of common date selectors for web scraping.
    
    Returns:
        List of CSS selectors
    """
    return [
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
        
        # Common ID attributes
        '#publish-date',
        '#post-date',
        '#article-date',
    ]


def format_date_for_display(date: Optional[datetime]) -> str:
    """
    Format date for human-readable display.
    
    Args:
        date: datetime object
        
    Returns:
        Formatted date string
    """
    if not date:
        return "Unknown date"
    
    return date.strftime("%B %d, %Y at %I:%M %p %Z")


def get_age_description(date: Optional[datetime]) -> str:
    """
    Get human-readable age description.
    
    Args:
        date: datetime object
        
    Returns:
        Age description (e.g., "2 hours ago", "yesterday")
    """
    if not date:
        return "Unknown"
    
    now = datetime.now(timezone.utc)
    
    if date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)
    
    delta = now - date
    
    seconds = delta.total_seconds()
    
    if seconds < 60:
        return "Just now"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    elif seconds < 172800:  # 48 hours
        return "Yesterday"
    elif seconds < 604800:  # 7 days
        days = int(seconds / 86400)
        return f"{days} days ago"
    else:
        return date.strftime("%B %d, %Y")


# Convenience functions
def parse_date(date_input: Union[str, datetime, None]) -> Optional[datetime]:
    """Convenience function for DateParser.parse()"""
    return DateParser.parse(date_input)


def test_date_parser():
    """Test date parser with various formats"""
    print("\n" + "="*60)
    print("TESTING DATE PARSER")
    print("="*60)
    
    test_dates = [
        "2024-11-03T10:30:00Z",
        "2024-11-03T10:30:00",
        "2024-11-03",
        "November 3, 2024",
        "Nov 3, 2024",
        "11/03/2024",
        "03-11-2024",
        "2 hours ago",
        "yesterday",
        "today",
        "Invalid date string"
    ]
    
    for date_str in test_dates:
        parsed = parse_date(date_str)
        if parsed:
            is_recent = is_within_last_24_hours(parsed)
            age = get_age_description(parsed)
            print(f"✓ '{date_str}'")
            print(f"  → {parsed.isoformat()}")
            print(f"  → Recent: {is_recent}, Age: {age}")
        else:
            print(f"✗ '{date_str}' → Could not parse")
        print()
    
    print("="*60 + "\n")


if __name__ == "__main__":
    test_date_parser()