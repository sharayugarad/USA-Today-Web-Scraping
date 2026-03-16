#!/bin/bash
# run_product_recall.sh

# 1. Go to project directory
cd /home/deepak/CodeLab/GitHub/Mass-Arbitration-Web-Scraping || exit 1

# 2. Activate virtualenv
source zvenv/bin/activate

# 3. Run the main scraper
python3 main.py >> logs/main_scraper.log 2>&1 &

# 3. Run the generic scraper
python3 main_generic.py >> logs/generic_scraper.log 2>&1 &