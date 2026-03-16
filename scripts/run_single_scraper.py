#!/usr/bin/env python3
"""
Run Single Scraper
Convenience script to run a specific scraper by name.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.orchestrator_og import ScraperOrchestrator


def main():
    """Run a single scraper"""
    if len(sys.argv) < 2:
        print("\nUsage: python scripts/run_single_scraper.py <scraper_name>")
        print("\nAvailable scrapers:")
        print("  - clarkson")
        print("  - forthepeople")
        print("  - lowey")
        print("  - classaction_org")
        print("  - berger_montague")
        print("  - lantern_labaton")
        print("  - siri_llp")
        print("  - zr_claims")
        print("  - crosner")
        print("  - shamis_gentile")
        print("  - toppe_firm")
        print("  - consumer_protection")
        print("  - ny_ag")
        print("  - tx_ag")
        sys.exit(1)
    
    scraper_name = sys.argv[1]
    
    print("\n" + "="*60)
    print(f"Running Scraper: {scraper_name}")
    print("="*60 + "\n")
    
    orchestrator = ScraperOrchestrator()
    result = orchestrator.run_specific_scraper(scraper_name)
    
    if result is None:
        print(f"\n❌ Scraper '{scraper_name}' not found!")
        sys.exit(1)
    
    orchestrator.print_summary()


if __name__ == "__main__":
    main()