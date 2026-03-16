#!/usr/bin/env python3
"""
Email Notifier Module
Handles email notifications for scraped URLs
"""

import smtplib
import logging
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
from typing import List, Dict, Optional
from datetime import datetime

from config.settings import EMAIL_CONFIG


class EmailNotifier:
    """
    Handles email notifications for new URLs with dynamic subject lines.
    Enhanced security and validation.
    """
    
    def __init__(self):
        """Initialize email notifier with configuration"""
        self.logger = logging.getLogger(__name__)
        
        # Load email configuration
        self.smtp_server = EMAIL_CONFIG.get('smtp_server', '')
        self.smtp_port = EMAIL_CONFIG.get('smtp_port', 587)
        self.use_ssl = EMAIL_CONFIG.get('use_ssl', False)
        self.sender_email = EMAIL_CONFIG.get('sender_email', '')
        self.sender_password = EMAIL_CONFIG.get('sender_password', '')
        self.receiver_emails = EMAIL_CONFIG.get('receiver_emails', [])
        
        self.logger.info(f"Email notifier initialized")
        self.logger.info(f"SMTP: {self.smtp_server}:{self.smtp_port}")
        self.logger.info(f"Sender: {self.sender_email}")
        self.logger.info(f"Recipients: {len(self.receiver_emails)}")

    def send_digest_email(self, subject: str, body: str, digest_data: dict, total_urls: int) -> bool:
        """
        Send a daily digest email with all URLs from all scrapers.
        
        Args:
            subject (str): Email subject
            body (str): HTML email body
            digest_data (dict): Dictionary of scraper_name -> urls
            total_urls (int): Total number of URLs
            
        Returns:
            bool: True if sent successfully
        """
        try:
            self.logger.info(f"Sending daily digest email with {total_urls} URLs")
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.receiver_emails)
            msg['Subject'] = subject
            msg['Date'] = formatdate(localtime=True)
            
            # Attach HTML body
            html_part = MIMEText(body, 'html')
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=30) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            
            self.logger.info("Daily digest email sent successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to send digest email: {e}")
            return False

    def _is_valid_email(self, email: str) -> bool:
        """
        Validate email format.
        
        Args:
            email (str): Email address to validate
            
        Returns:
            bool: True if valid email format
        """
        if not email or not isinstance(email, str):
            return False
        
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email.strip()) is not None

    def _is_config_complete(self):
        """
        Check that all necessary config fields are present.
        
        Returns:
            bool: True if configuration is complete
        """
        required_fields = {
            'sender_email': self.sender_email,
            'sender_password': self.sender_password,
            'smtp_server': self.smtp_server,
            'smtp_port': self.smtp_port,
            'receiver_emails': self.receiver_emails,
        }
        
        missing_fields = [k for k, v in required_fields.items() if not v]
        
        if missing_fields:
            print(f"ERROR: Missing configuration fields: {', '.join(missing_fields)}")
            return False
        
        return True


def test_email_config():
    """Test email configuration."""
    print("\n" + "="*60)
    print("TESTING EMAIL CONFIGURATION")
    print("="*60)
    
    notifier = EmailNotifier()
    
    if not notifier._is_config_complete():
        print("\nERROR: Configuration incomplete - cannot send test email")
        return False
    
    print("\nCurrent Configuration:")
    print(f"   SMTP Server: {notifier.smtp_server}:{notifier.smtp_port}")
    print(f"   Sender: {notifier.sender_email}")
    print(f"   Recipients: {', '.join(notifier.receiver_emails)}")
    print(f"   SSL: {notifier.use_ssl}")
    
    # Ask user if they want to send test email
    response = input("\nDo you want to send a test email? (yes/no): ").strip().lower()
    
    if response not in ['yes', 'y']:
        print("Test email cancelled.")
        return True
    
    # Send test digest
    test_body = """
    <html>
    <body>
        <h1>Test Email - Mass ARB Scraper</h1>
        <p>This is a test email to verify your configuration is working.</p>
        <p>If you received this, your email setup is correct! ✅</p>
    </body>
    </html>
    """
    
    print("\nSending test email...")
    success = notifier.send_digest_email(
        subject="Test Email - Mass ARB Scraper Configuration",
        body=test_body,
        digest_data={},
        total_urls=0
    )
    
    if success:
        print("\n✅ Email sent successfully!")
        print("   Check your inbox for the test email")
    else:
        print("\n❌ Email configuration has issues. Please check the errors above.")
    
    print("="*60 + "\n")
    return success


if __name__ == "__main__":
    test_email_config()