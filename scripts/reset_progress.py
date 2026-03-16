#!/usr/bin/env python3
"""
Reset Progress
Reset batch progress and last run times (use with caution).
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.batch_processor import BatchProcessor


def main():
    """Reset progress files"""
    print("\n" + "="*60)
    print("⚠️  RESET PROGRESS")
    print("="*60)
    print("\nThis will reset:")
    print("  - Batch progress (sent URLs)")
    print("  - Last run times for all scrapers")
    print("\nAll URLs will be sent again on next run!")
    
    response = input("\n❓ Are you sure? Type 'yes' to confirm: ").strip().lower()
    
    if response != 'yes':
        print("\n✅ Reset cancelled.")
        sys.exit(0)
    
    processor = BatchProcessor()
    
    print("\nResetting batch progress...")
    processor.reset_progress()
    
    print("Resetting last run times...")
    processor.reset_last_run_times()
    
    print("\n✅ Progress reset complete!")
    print("Next scraper run will process all URLs as if it's the first run.\n")


if __name__ == "__main__":
    main()