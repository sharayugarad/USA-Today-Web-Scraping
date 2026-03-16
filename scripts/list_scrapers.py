#!/usr/bin/env python3
"""
List All Scrapers
Display all available scrapers and their status.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.orchestrator_og import ScraperOrchestrator


def main():
    """List all available scrapers"""
    orchestrator = ScraperOrchestrator()
    orchestrator.list_all_scrapers()


if __name__ == "__main__":
    main()