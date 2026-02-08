"""
Process Top 100 Leads from FRESH Intelligence Data

This version works directly with fresh_intelligence.csv (today's scraped data)
instead of the old leads_export.csv
"""

import csv
from pathlib import Path
from operator import itemgetter
from datetime import datetime


def process_fresh_top_leads(input_csv: str, output_csv: str, top_n: int = 100):
    """Process only top N leads from FRESH scraped data by confidence score"""
    
    print(f"Loading FRESH leads from {input_csv}...")
    
    # Read all leads
    leads = []
    with open(input_csv, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert confidence to float for sorting
            try:
                row['confidence_float'] = float(row.get('confidence', 0))
            except:
                row['confidence_float'] = 0.0
            leads.append(row)
    
    print(f"Total FRESH leads loaded: {len(leads)}")
    
    # Sort by confidence (descending) and take top N
    leads_sorted = sorted(leads, key=itemgetter('confidence_float'), reverse=True)
    top_leads = leads_sorted[:top_n]
    
    print(f"Processing top {len(top_leads)} fresh leads...")
    
    # Write to output
    if top_leads:
        fieldnames = [k for k in top_leads[0].keys() if k != 'confidence_float']
        
        with open(output_csv, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for lead in top_leads:
                # Remove the helper field
                lead_copy = {k: v for k, v in lead.items() if k != 'confidence_float'}
                writer.writerow(lead_copy)
    
    print(f"✓ Saved {len(top_leads)} top FRESH leads to {output_csv}")
    return len(top_leads)


if __name__ == '__main__':
    base_path = Path(__file__).parent
    
    # Use FRESH intelligence data scraped today
    input_file = base_path / 'data' / 'fresh_intelligence.csv'
    output_file = base_path / 'data' / 'top_100_fresh_leads.csv'
    
    if not input_file.exists():
        print(f"ERROR: {input_file} not found!")
        print("Run scraping first:")
        print("  python main.py scrape")
        print("  python main.py export --output fresh_intelligence")
        exit(1)
    
    count = process_fresh_top_leads(str(input_file), str(output_file), top_n=100)
    
    print(f"\n{'='*60}")
    print(f"SUCCESS: Processed {count} top FRESH leads")
    print(f"Source: fresh_intelligence.csv (TODAY'S DATA)")
    print(f"Output: {output_file}")
    print(f"{'='*60}")
