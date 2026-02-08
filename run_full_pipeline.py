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
from pathlib import Path

# Run filter_leads.py first
print("=" * 70)
print("STEP 1: FILTERING AND SCORING LEADS")
print("=" * 70)
print()

from filter_leads import main as filter_main
filter_main()

# Then map officers
print("\n" + "=" * 70)
print("STEP 2: MAPPING HPCL OFFICERS TO LEADS")
print("=" * 70)
print()

from map_officers_to_leads import main as officer_main
result = officer_main()

print("\n" + "=" * 70)
print("✅ COMPLETE PIPELINE FINISHED SUCCESSFULLY!")
print("=" * 70)
print()
print("Your filtered_dataset.csv now includes:")
print("  ✓ Lead scoring and prioritization")
print("  ✓ Product recommendations")
print("  ✓ HPCL officer contact information")
print()
print("Ready for WhatsApp notification system! 🚀")
print()

sys.exit(result if result else 0)
