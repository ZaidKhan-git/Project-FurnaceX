"""
Raw Data Aggregator
Purpose: Unify all data sources into a single raw_data.csv file
Input: Environmental data (scraped_data), Intelligence data (existing leads_export.csv)
Output: Unified raw_data.csv with consistent schema

OPTIMIZED: Removed raw_json column to prevent OOM crashes
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import gc
import re

class RawDataAggregator:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.current_time = datetime.now().isoformat()
        self.all_raw_records = []
        
    def aggregate_environmental_data(self):
        """
        Load environmental clearance data from scraped_data folders.
        Uses Proposal No. as the unique ID.
        """
        print("=" * 70)
        print("AGGREGATING ENVIRONMENTAL CLEARANCE DATA")
        print("=" * 70)
        
        scraped_data_dir = self.base_path / 'data' / 'scraped_data'
        
        if not scraped_data_dir.exists():
            print(f"⚠ Warning: {scraped_data_dir} not found")
            return
        
        count = 0
        for state_dir in scraped_data_dir.iterdir():
            if not state_dir.is_dir():
                continue
                
            state_name = state_dir.name.replace('_', ' ').title()
            
            for csv_file in state_dir.glob("*.csv"):
                try:
                    df = pd.read_csv(csv_file, encoding='utf-8', on_bad_lines='skip')
                    
                    for _, row in df.iterrows():
                        # Create unified raw record (OPTIMIZED: no raw_json)
                        raw_record = {
                            'id': row.get('Proposal No.', f'ENV-UNKNOWN-{count}'),
                            'source_system': 'parivesh',
                            'company_name': row.get('Project Proponent', ''),
                            'project_name': row.get('Project Name', ''),
                            'location': row.get('Location', ''),
                            'state': state_name,
                            'sector': self._extract_sector(row.get('Other Details', '')),
                            'status': row.get('Proposal Status', ''),
                            'description': row.get('Project_Description', ''),
                            'source_url': '',
                            'discovered_at': self.current_time
                        }
                        
                        self.all_raw_records.append(raw_record)
                        count += 1
                    
                    print(f"  ✓ Loaded {len(df)} records from {state_dir.name}/{csv_file.name}")
                    del df  # Free memory immediately
                    
                except Exception as e:
                    print(f"  ✗ Error loading {csv_file}: {e}")
        
        gc.collect()  # Force garbage collection
        print(f"\nTotal environmental records: {count}")
        
    def aggregate_intelligence_data(self):
        """
        Load intelligence data from fresh export AND backup.
        Generates new IDs in format INT-{SOURCE}-{SEQ}.
        """
        print("\n" + "=" * 70)
        print("AGGREGATING INTELLIGENCE DATA")
        print("=" * 70)
        
        # Files to load (Fresh first, then backup)
        files = [
            # Fresh data from today's scrape
            self.base_path / 'data' / 'fresh_intelligence.csv',
            # Backup of legacy data
            self.base_path / 'data' / 'leads_export_backup.csv'
        ]
        
        total_loaded = 0
        
        for file_path in files:
            if not file_path.exists():
                print(f"ℹ Note: {file_path.name} not found")
                continue
            
            try:
                # Handle potential BOM in Excel-saved CSVs
                df = pd.read_csv(file_path, encoding='utf-8-sig')
                
                print(f"  Processing {file_path.name}...")
                
                count = 0
                for _, row in df.iterrows():
                    # Generate formatted ID based on source
                    source = str(row.get('source', 'UNKNOWN')).upper()
                    existing_id = row.get('id', '')
                    
                    # Create new ID format: INT-{SOURCE}-{ORIGINAL_ID}
                    # Check if ID already has "INT-" prefix (avoid double prefixing)
                    if str(existing_id).startswith('INT-'):
                        new_id = existing_id
                    else:
                        new_id = f"INT-{source}-{existing_id}"
                    
                    # Create unified raw record (OPTIMIZED: no raw_json)
                    raw_record = {
                        'id': new_id,
                        'source_system': source.lower(),
                        'company_name': row.get('company_name', ''),
                        'project_name': row.get('signal_type', ''),
                        'location': row.get('territory', ''),
                        'state': self._extract_state_from_location(row),
                        'sector': row.get('sector', ''),
                        'status': row.get('status', 'NEW'),
                        'description': row.get('keywords_summary', ''),
                        'source_url': row.get('source_url', ''),
                        'discovered_at': row.get('discovered_at', self.current_time)
                    }
                    
                    self.all_raw_records.append(raw_record)
                    count += 1
                
                print(f"  ✓ Loaded {count} intelligence records from {file_path.name}")
                total_loaded += count
                
            except Exception as e:
                print(f"  ✗ Error loading {file_path.name}: {e}")
        
        gc.collect()  # Force garbage collection
        print(f"\nTotal intelligence records: {total_loaded}")
    
    def _extract_sector(self, other_details):
        """Extract sector from Other Details field"""
        if pd.isna(other_details):
            return 'Unknown'
        match = re.search(r'Sector:\s*([A-Za-z0-9\-]+)', str(other_details))
        return match.group(1) if match else 'Unknown'
    
    def _extract_state_from_location(self, row):
        """Try to extract state from various location fields"""
        # Try raw_data JSON first
        try:
            raw_data = json.loads(row.get('raw_data', '{}'))
            if 'location' in raw_data:
                return raw_data['location']
        except:
            pass
        
        # Fallback to territory or empty
        return row.get('territory', '').replace(' Region', '')
    
    def save_raw_data(self):
        """Save unified raw data to CSV"""
        print("\n" + "=" * 70)
        print("SAVING UNIFIED RAW DATA")
        print("=" * 70)
        
        if not self.all_raw_records:
            print("⚠ No records to save!")
            return
        
        # Create DataFrame
        df = pd.DataFrame(self.all_raw_records)
        
        # Remove duplicates based on ID
        original_count = len(df)
        df = df.drop_duplicates(subset=['id'], keep='first')
        removed = original_count - len(df)
        
        # Save to CSV
        output_path = self.base_path / 'data' / 'raw_data.csv'
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"\n✓ Saved {len(df)} unique records to {output_path}")
        if removed > 0:
            print(f"  (Removed {removed} duplicate IDs)")
        
        # Print summary statistics
        print("\n" + "=" * 70)
        print("SUMMARY STATISTICS")
        print("=" * 70)
        print(f"Total unique records: {len(df)}")
        print(f"\nBy Source System:")
        print(df['source_system'].value_counts())
        print(f"\nBy State:")
        print(df['state'].value_counts().head(10))
        
    def run(self):
        """Execute full aggregation pipeline"""
        print("\n" + "=" * 70)
        print("RAW DATA AGGREGATION PIPELINE")
        print("=" * 70)
        print(f"Timestamp: {self.current_time}\n")
        
        self.aggregate_environmental_data()
        self.aggregate_intelligence_data()
        self.save_raw_data()
        
        print("\n" + "=" * 70)
        print("✓ RAW DATA AGGREGATION COMPLETE!")
        print("=" * 70)

def main():
    """Main execution"""
    base_path = Path(__file__).parent
    aggregator = RawDataAggregator(base_path)
    aggregator.run()

if __name__ == '__main__':
    main()
