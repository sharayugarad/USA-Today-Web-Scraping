#!/usr/bin/env python3
"""
Run All Scrapers
Convenience script to run all enabled scrapers.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.orchestrator_og import ScraperOrchestrator


def main():
    """Run all enabled scrapers"""
    print("\n" + "="*60)
    print("Running All Scrapers")
    print("="*60 + "\n")
    
    orchestrator = ScraperOrchestrator()
    orchestrator.run_all_scrapers()
    orchestrator.print_summary()


if __name__ == "__main__":
    main()