"""
Lead Processor
Purpose: Transform raw_data.csv into filtered, enriched leads_export.csv
Input: raw_data.csv (unified raw data from all sources)
Output: leads_export.csv (filtered leads with metadata and keywords)
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import json

class LeadProcessor:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.raw_data = None
        self.processed_leads = []
        
        # Signal type classification
        self.signal_patterns = {
            'Environmental Clearance': [r'clearance', r'EC', r'ToR', r'SEIAA', r'MOEF'],
            'Government Tender': [r'tender', r'NIT', r'RFP', r'RFQ', r'GeM'],
            'Capacity Expansion': [r'expansion', r'augment', r'capacity addition', r'greenfield'],
            'PSU Procurement': [r'procurement', r'DG Set', r'Storage Tank', r'Boiler'],
            'New Company Registration': [r'registration', r'MCA', r'authorized capital'],
            'Credit Rating': [r'rating', r'CARE', r'CRISIL', r'ICRA'],
            'Financial Announcement': [r'CAPEX', r'Board approves', r'expansion project']
        }
        
        # Keyword extraction patterns
        self.keyword_patterns = {
            'industrial_machinery': [
                r'DG Set', r'Storage Tank', r'Boiler', r'Captive Power Plant',
                r'Thermic Fluid Heater', r'Industrial Boilers'
            ],
            'regulatory_events': [
                r'NIT', r'Procurement', r'Expansion', r'Greenfield Project',
                r'CTO', r'Supply of'
            ],
            'commodities': [
                r'HSD', r'Diesel', r'Bitumen', r'VG-30', r'Furnace Oil',
                r'Mineral Turpentine Oil'
            ]
        }
        
    def load_raw_data(self):
        """Load raw_data.csv"""
        print("=" * 70)
        print("LOADING RAW DATA")
        print("=" * 70)
        
        raw_data_path = self.base_path / 'data' / 'raw_data.csv'
        
        if not raw_data_path.exists():
            raise FileNotFoundError(f"raw_data.csv not found at {raw_data_path}")
        
        self.raw_data = pd.read_csv(raw_data_path, encoding='utf-8')
        print(f"✓ Loaded {len(self.raw_data)} raw records\n")
        
    def classify_signal_type(self, record):
        """Classify the signal type based on content"""
        text = f"{record.get('project_name', '')} {record.get('description', '')} {record.get('status', '')}".lower()
        
        for signal, patterns in self.signal_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    return signal
        
        # Default based on source system
        source = record.get('source_system', '').lower()
        if source == 'parivesh':
            return 'Environmental Clearance'
        elif source == 'gem':
            return 'Government Tender'
        elif source == 'bse':
            return 'Financial Announcement'
        elif source == 'mca':
            return 'New Company Registration'
        elif source in ['care', 'crisil', 'icra']:
            return 'Credit Rating'
        
        return 'Other'
    
    def extract_keywords(self, record):
        """Extract and categorize keywords from description"""
        text = f"{record.get('project_name', '')} {record.get('description', '')}".lower()
        keywords = {}
        
        for category, patterns in self.keyword_patterns.items():
            matches = []
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    matches.append(pattern)
            if matches:
                keywords[category] = matches
        
        return json.dumps(keywords) if keywords else '{}'
    
    def infer_product_match(self, record):
        """Infer product matches based on sector and keywords"""
        sector = record.get('sector', '').upper()
        text = f"{record.get('project_name', '')} {record.get('description', '')}".lower()
        
        products = set()
        
        # Sector-based inference
        if 'MIN' in sector or 'MINING' in sector.upper():
            products.update(['Diesel', 'Lubricants'])
        elif 'INFRA' in sector:
            products.update(['Bitumen', 'Diesel', 'HSD'])
        elif 'IND' in sector or 'INDUSTRIAL' in sector.upper():
            products.update(['Furnace Oil', 'Diesel', 'Lubricants'])
        
        # Keyword-based inference
        if re.search(r'bitumen|VG-30|road', text, re.IGNORECASE):
            products.add('Bitumen')
        if re.search(r'diesel|HSD|fuel', text, re.IGNORECASE):
            products.add('Diesel')
        if re.search(r'furnace oil|boiler|thermal', text, re.IGNORECASE):
            products.add('Furnace Oil')
        if re.search(r'lubricant|lubrication', text, re.IGNORECASE):
            products.add('Lubricants')
        
        return ', '.join(sorted(products)) if products else ''
    
    def calculate_confidence(self, record):
        """Calculate confidence score based on data completeness"""
        score = 0.0
        
        # Check key fields
        if record.get('company_name'):
            score += 20
        if record.get('project_name'):
            score += 20
        if record.get('description'):
            score += 20
        if record.get('source_url'):
            score += 20
        if record.get('state'):
            score += 10
        if record.get('sector') and record.get('sector') != 'Unknown':
            score += 10
        
        return score
    
    def process_records(self):
        """Process each raw record into enriched lead"""
        print("=" * 70)
        print("PROCESSING AND ENRICHING LEADS")
        print("=" * 70)
        
        for _, row in self.raw_data.iterrows():
            # Create enriched lead record
            lead = {
                'id': row['id'],
                'company_name': row.get('company_name', ''),
                'signal_type': self.classify_signal_type(row),
                'source': row.get('source_system', ''),
                'source_url': row.get('source_url', ''),
                'keywords': self.extract_keywords(row),
                'sector': row.get('sector', 'Unknown'),
                'product_match': self.infer_product_match(row),
                'confidence': self.calculate_confidence(row),
                'status': row.get('status', 'NEW'),
                'state': row.get('state', ''),
                'location': row.get('location', ''),
                'project_name': row.get('project_name', ''),
                'description': row.get('description', ''),
                'discovered_at': row.get('discovered_at', '')
            }
            
            self.processed_leads.append(lead)
        
        print(f"✓ Processed {len(self.processed_leads)} leads\n")
    
    def export_leads(self):
        """Save processed leads to leads_export.csv"""
        print("=" * 70)
        print("EXPORTING PROCESSED LEADS")
        print("=" * 70)
        
        if not self.processed_leads:
            print("⚠ No leads to export!")
            return
        
        df = pd.DataFrame(self.processed_leads)
        
        # Reorder columns
        column_order = [
            'id', 'company_name', 'signal_type', 'source', 'source_url',
            'keywords', 'sector', 'product_match', 'confidence', 'status',
            'state', 'location', 'project_name', 'description', 'discovered_at'
        ]
        
        df = df[column_order]
        
        # Save to CSV
        output_path = self.base_path / 'data' / 'leads_export.csv'
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"✓ Saved {len(df)} leads to {output_path}\n")
        
        # Summary statistics
        print("=" * 70)
        print("SUMMARY STATISTICS")
        print("=" * 70)
        print(f"Total leads exported: {len(df)}")
        print(f"\nBy Signal Type:")
        print(df['signal_type'].value_counts())
        print(f"\nBy Source:")
        print(df['source'].value_counts())
        print(f"\nAverage Confidence Score: {df['confidence'].mean():.1f}")
        
    def run(self):
        """Execute full processing pipeline"""
        print("\n" + "=" * 70)
        print("LEAD PROCESSING PIPELINE")
        print("=" * 70)
        print(f"Timestamp: {datetime.now().isoformat()}\n")
        
        self.load_raw_data()
        self.process_records()
        self.export_leads()
        
        print("\n" + "=" * 70)
        print("✓ LEAD PROCESSING COMPLETE!")
        print("=" * 70)

def main():
    """Main execution"""
    base_path = Path(__file__).parent
    processor = LeadProcessor(base_path)
    processor.run()

if __name__ == '__main__':
    main()
