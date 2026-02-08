"""
Lightweight Top Leads Filter
NO PANDAS - Pure Python CSV processing for top 100 leads

This bypasses the pandas import issue entirely.
"""

import csv
from pathlib import Path
from operator import itemgetter


def process_top_leads(input_csv: str, output_csv: str, top_n: int = 100):
    """Process only top N leads by confidence score"""
    
    print(f"Loading leads from {input_csv}...")
    
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
    
    print(f"Total leads loaded: {len(leads)}")
    
    # Sort by confidence (descending) and take top N
    leads_sorted = sorted(leads, key=itemgetter('confidence_float'), reverse=True)
    top_leads = leads_sorted[:top_n]
    
    print(f"Processing top {len(top_leads)} leads...")
    
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
    
    print(f"✓ Saved {len(top_leads)} top leads to {output_csv}")
    return len(top_leads)


if __name__ == '__main__':
    base_path = Path(__file__).parent
    input_file = base_path / 'data' / 'leads_export.csv'
    output_file = base_path / 'data' / 'top_100_leads.csv'
    
    count = process_top_leads(str(input_file), str(output_file), top_n=100)
    print(f"\n{'='*60}")
    print(f"SUCCESS: Processed {count} top leads")
    print(f"Output: {output_file}")
    print(f"{'='*60}")
