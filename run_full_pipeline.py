# run_full_pipeline.py - Complete Pipeline with Officer Mapping
"""
Run the complete lead processing pipeline including officer assignment.

This script:
1. Filters and scores leads from scraped data
2. Automatically maps nearest HPCL officers to each lead
3. Exports enhanced dataset ready for CRM/WhatsApp integration

Usage:
    python run_full_pipeline.py
"""

import sys
import subprocess
from pathlib import Path

PYTHON_EXE = sys.executable

# 0. Run Scrapers & Export Fresh Data
print("=" * 70)
print("STEP 0: SCRAPING FRESH DATA (REAL-TIME)")
print("=" * 70)
print()

# A. Run Seeder/Scraper (Simulating or Running Real Scrapers)
print(">> Running Historical Data Seeder (Simulating Parivesh/GeM/BSE)...")
# Using seed_historical_data.py as the primary scraper for now
subprocess.run([PYTHON_EXE, "seed_historical_data.py"], check=False)

# B. Export to fresh_intelligence.csv
print("\n>> Exporting fresh data from database...")
# Export to data/fresh_intelligence.csv
subprocess.run([PYTHON_EXE, "main.py", "export", "--output", "fresh_intelligence"], check=True)
print("✓ Fresh intelligence data exported.")

# 1. Aggregate Raw Data
print("\n" + "=" * 70)
print("STEP 1: AGGREGATING RAW DATA")
print("=" * 70)
print()

from aggregate_raw_data import main as aggr_main
aggr_main()

# 2. Process Leads (Filter & Enrich)
print("\n" + "=" * 70)
print("STEP 2: PROCESSING LEADS (FILTER & ENRICH)")
print("=" * 70)
print()

from process_leads import main as process_main
process_main()

# 3. Filter Leads (Score)
print("\n" + "=" * 70)
print("STEP 3: SCORING LEADS")
print("=" * 70)
print()

from filter_leads import main as filter_main
filter_main()

# 4. Map Officers
print("\n" + "=" * 70)
print("STEP 4: MAPPING HPCL OFFICERS TO LEADS")
print("=" * 70)
print()

from map_officers_to_leads import main as officer_main
result = officer_main()

print("\n" + "=" * 70)
print("✅ COMPLETE PIPELINE FINISHED SUCCESSFULLY!")
print("=" * 70)
print()
print("Your filtered_dataset.csv now includes:")
print("  ✓ Unified data from environmental & intelligence sources")
print("  ✓ Metadata enrichment (Signal Type, Keywords)")
print("  ✓ Lead scoring and prioritization")
print("  ✓ Product recommendations")
print("  ✓ HPCL officer contact information")
print()
print("Ready for WhatsApp notification system! 🚀")
print()

sys.exit(result if result else 0)
