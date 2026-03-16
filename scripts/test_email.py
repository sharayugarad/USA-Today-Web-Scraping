#!/usr/bin/env python3
"""
Test Email Configuration
Script to test email notification settings.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.notifier import test_email_config


def main():
    """Test email configuration"""
    print("\n" + "="*60)
    print("TESTING EMAIL CONFIGURATION")
    print("="*60 + "\n")
    
    success = test_email_config()
    
    if success:
        print("\n✅ Email configuration is working!")
        sys.exit(0)
    else:
        print("\n❌ Email configuration has issues. Please check the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()