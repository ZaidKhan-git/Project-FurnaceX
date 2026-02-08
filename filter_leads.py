"""
Lead Filtering & Export Tool
Purpose: Filter and score leads from scraped environmental clearance data
Output: Agent-ready CSV with prioritized leads and product recommendations
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import json

class LeadFilter:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.all_leads = []
        self.current_date = datetime(2026, 2, 8)  # Reference date for freshness calculation
        
        # Product mapping rules
        self.product_map = {
            'MIN': ['Diesel', 'Lubricants', 'Transportation Fuel'],
            'Non-Coal': ['Diesel', 'Lubricants', 'Transportation Fuel'],
            'INFRA-2': ['Diesel', 'Bitumen', 'Lubricants', 'Generator Fuel'],
            'INFRA-1': ['Bitumen', 'Diesel', 'Road Construction Materials'],
            'Industrial': ['Furnace Oil', 'Diesel', 'Lubricants'],
            'Thermal': ['Furnace Oil', 'Diesel', 'Specialty Fuels'],
        }
        
        # Intent strength keyword patterns (Weight: 0.4)
        self.high_intent_keywords = {
            r'\btender\b': 1.0,
            r'\bprocurement\b': 1.0,
            r'\bbid\b': 1.0,
            r'\bRFP\b': 1.0,
            r'\bRFQ\b': 1.0,
            r'\bEOI\b': 0.9,
            r'\bcontract\b': 0.9,
            r'\bsupply agreement\b': 0.85,
            r'\bfuel supply\b': 0.85,
        }
        
        self.medium_intent_keywords = {
            r'\bexpansion\b': 0.7,
            r'\bnew plant\b': 0.7,
            r'\bgreenfield\b': 0.75,
            r'\bcapacity addition\b': 0.7,
            r'\bupgrade\b': 0.65,
            r'\bmodernization\b': 0.65,
            r'\binstallation\b': 0.6,
        }
        
        self.low_intent_keywords = {
            r'\bproposed\b': 0.3,
            r'\bplanned\b': 0.3,
            r'\bunder consideration\b': 0.2,
        }
        
        # Load HPCL depot proximity data
        self.proximity_scores = self._load_proximity_data()
        
    def load_all_data(self):
        """Load pre-processed leads from leads_export.csv"""
        print("Loading pre-processed leads from leads_export.csv...")
        
        leads_export_path = self.base_path / 'leads_export.csv'
        
        if not leads_export_path.exists():
            raise FileNotFoundError(f"leads_export.csv not found at {leads_export_path}")
        
        # Load the preprocessed data
        df = pd.read_csv(leads_export_path, encoding='utf-8')
        
        # Map leads_export columns to expected format
        df['Proposal No.'] = df['id']
        df['Project Name'] = df['project_name']
        df['Project Proponent'] = df['company_name']
        df['Location'] = df['location']
        df['State'] = df['state']
        df['Proposal Status'] = df['status']
        df['Project_Description'] = df['description']
        
        # Add new columns from leads_export
        df['Company_Name'] = df['company_name']
        df['Signal_Type'] = df['signal_type']
        df['Source_URL'] = df['source_url']
        df['Source_System'] = df['source']
        
        # Extract sector and category from existing sector field
        df['Sector'] = df['sector'].fillna('Unknown')
        df['Category'] = df['sector'].apply(lambda x: 'A' if 'IND' in str(x).upper() else 'B1')
        
        # Parse discovered_at to get date string (DD/MM/YYYY) for legacy compatibility
        try:
            df['Discovered_Date'] = pd.to_datetime(df['discovered_at']).dt.strftime('%d/%m/%Y')
        except:
            # Fallback to current date if parsing fails
            from datetime import datetime
            df['Discovered_Date'] = datetime.now().strftime('%d/%m/%Y')
        
        # Store other details in expected format (Critical: Must include 'Date of Submission' for scoring)
        df['Other Details'] = df.apply(
            lambda row: f"Category: {row['Category']} Sector: {row['Sector']} Source: {row['source']} Date of Submission: {row['Discovered_Date']}", 
            axis=1
        )
        
        self.combined_df = df
        
        print(f"\nTotal records loaded: {len(self.combined_df)}")
        print(f"Unique leads: {len(self.combined_df)}\n")
        
    def extract_sector(self, other_details):
        """Extract sector from Other Details field"""
        if pd.isna(other_details):
            return 'Unknown'
        match = re.search(r'Sector:\s*([A-Za-z0-9\-]+)', str(other_details))
        return match.group(1) if match else 'Unknown'
    
    def extract_category(self, other_details):
        """Extract category from Other Details field"""
        if pd.isna(other_details):
            return 'Unknown'
        match = re.search(r'Category:\s*([A-Za-z0-9]+)', str(other_details))
        return match.group(1) if match else 'Unknown'
    
    def extract_submission_year(self, other_details):
        """Extract submission year"""
        if pd.isna(other_details):
            return None
        match = re.search(r'Date of Submission:\s*\d{2}/\d{2}/(\d{4})', str(other_details))
        return int(match.group(1)) if match else None
    
    def _load_proximity_data(self):
        """Load HPCL depot proximity scores from config"""
        try:
            config_path = Path('config/hpcl_depots.json')
            if config_path.exists():
                with open(config_path, 'r') as f:
                    data = json.load(f)
                    return data.get('state_proximity_scores', {})
        except Exception as e:
            print(f"Warning: Could not load HPCL depot data: {e}")
        # Default fallback scores
        return {}
    
    def parse_full_date(self, other_details):
        """Parse full submission date from Other Details (DD/MM/YYYY format)"""
        if pd.isna(other_details):
            return None
        match = re.search(r'Date of Submission:\s*(\d{2})/(\d{2})/(\d{4})', str(other_details))
        if match:
            try:
                day, month, year = match.groups()
                return datetime(int(year), int(month), int(day))
            except (ValueError, TypeError):
                return None
        return None
    
    def calculate_intent_strength(self, description):
        """Calculate intent strength score (0-1) based on keywords in project description"""
        if pd.isna(description):
            return 0.3  # Default for vague/missing descriptions
        
        description_lower = str(description).lower()
        max_score = 0.3  # Base score
        
        # Check high intent keywords first (highest priority)
        for pattern, score in self.high_intent_keywords.items():
            if re.search(pattern, description_lower, re.IGNORECASE):
                max_score = max(max_score, score)
        
        # Check medium intent keywords
        for pattern, score in self.medium_intent_keywords.items():
            if re.search(pattern, description_lower, re.IGNORECASE):
                max_score = max(max_score, score)
        
        # Check low intent keywords (only if no higher match)
        if max_score == 0.3:
            for pattern, score in self.low_intent_keywords.items():
                if re.search(pattern, description_lower, re.IGNORECASE):
                    max_score = max(max_score, score)
        
        return max_score
    
    def calculate_freshness(self, other_details):
        """Calculate freshness score (0-1) based on time decay from submission date"""
        submission_date = self.parse_full_date(other_details)
        
        if submission_date is None:
            return 0.5  # Default for missing dates
        
        days_since = (self.current_date - submission_date).days
        
        if days_since < 0:
            # Future date (data error), treat as very fresh
            return 1.0
        
        # Linear decay over 30 days: max(1 - days_since/30, 0)
        freshness = max(1 - (days_since / 30), 0)
        return freshness
    
    def calculate_size_proxy(self, description, category):
        """Calculate company/project size proxy (0-1) from capacity indicators"""
        if pd.isna(description):
            # Fallback to category
            return self._category_size_fallback(category)
        
        desc_str = str(description).lower()
        
        # Extract capacity values
        # Pattern 1: Power capacity (MW, GW)
        power_match = re.search(r'(\d+\.?\d*)\s*(mw|gw)', desc_str, re.IGNORECASE)
        if power_match:
            capacity = float(power_match.group(1))
            unit = power_match.group(2).lower()
            if unit == 'gw':
                capacity *= 1000  # Convert to MW
            
            if capacity >= 100:
                return 0.8
            elif capacity >= 50:
                return 0.6
            else:
                return 0.4
        
        # Pattern 2: Production capacity (TPA, MTPA - Tonnes Per Annum)
        production_match = re.search(r'(\d+\.?\d*)\s*(mtpa|million tpa|tpa)', desc_str, re.IGNORECASE)
        if production_match:
            capacity = float(production_match.group(1))
            unit = production_match.group(2).lower()
            
            # Normalize to MTPA
            if 'million' in unit or 'mtpa' in unit:
                # Already in millions
                pass
            else:
                capacity = capacity / 1_000_000  # Convert TPA to MTPA
            
            if capacity >= 1.0:
                return 0.8
            elif capacity >= 0.5:
                return 0.6
            else:
                return 0.4
        
        # Pattern 3: Area (Ha, Hectare)
        area_match = re.search(r'(\d+\.?\d*)\s*(ha|hectare)', desc_str, re.IGNORECASE)
        if area_match:
            area = float(area_match.group(1))
            
            if area >= 200:
                return 0.8
            elif area >= 100:
                return 0.6
            else:
                return 0.4
        
        # No capacity found, fall back to category
        return self._category_size_fallback(category)
    
    def _category_size_fallback(self, category):
        """Fallback size score based on project category"""
        if category == 'A':
            return 0.7
        elif category == 'B1':
            return 0.6
        elif category == 'B2':
            return 0.4
        else:
            return 0.5  # Unknown
    
    def calculate_proximity(self, location, state, project_desc=""):
        """Calculate geographic proximity score (0-1) to HPCL depots using actual distances"""
        try:
            from geocode_proximity import GeographicProximityScorer
            
            if not hasattr(self, 'geo_scorer'):
                self.geo_scorer = GeographicProximityScorer()
            
            return self.geo_scorer.calculate_proximity_score(location, state, project_desc)
        
        except Exception as e:
            print(f"  Warning: Geocoding failed for {location}, {state}: {e}")
            # Fallback to state-level scoring if geocoding fails
            return self.proximity_scores.get(str(state).strip(), 0.5)
    
    def calculate_lead_score(self, row):
        """Calculate lead score (0-100)"""
        score = 0
        
        # Recency (30 points)
        if row['Submission_Year'] == 2026:
            score += 30
        elif row['Submission_Year'] == 2025:
            score += 15
        
        # Category/Scale (25 points)
        if row['Category'] == 'A':
            score += 25
        elif row['Category'] == 'B1':
            score += 20
        elif row['Category'] == 'B2':
            score += 10
        
        # Sector/Demand (25 points)
        sector_scores = {
            'MIN': 25, 'Non-Coal': 25,
            'INFRA-2': 22, 'INFRA-1': 22,
            'Industrial': 18,
            'Thermal': 15
        }
        score += sector_scores.get(row['Sector'], 5)
        
        # Status (20 points)
        status_scores = {
            'Under Verification': 20,
            'Under Examination': 18,
            'Referred to SEIAA': 15,
            'Proposal Accepted and Referred to SEAC': 15,
            'Proposal Accepted and Referred to EAC': 15,
            'Under Processing with Authority': 12,
            'EDS Raised': 10,
            'ADS Raised': 10,
        }
        for status_key in status_scores:
            if status_key.lower() in str(row['Proposal Status']).lower():
                score += status_scores[status_key]
                break
        
        return min(100, score)
    
    def get_products(self, sector):
        """Get recommended products for sector"""
        # Try exact match first
        if sector in self.product_map:
            return ', '.join(self.product_map[sector])
        
        # Try partial matches
        for key in self.product_map:
            if key.lower() in sector.lower():
                return ', '.join(self.product_map[key])
        
        return 'Diesel, Lubricants'  # Default
    
    def assign_tier(self, score, status, year):
        """Assign priority tier"""
        if score >= 70 and year == 2026 and any(s in str(status) for s in ['Under Verification', 'Under Examination']):
            return 'Tier 1 - Immediate'
        elif score >= 55 and year == 2026:
            return 'Tier 2 - High Priority'
        elif score >= 40:
            return 'Tier 3 - Monitor'
        else:
            return 'Tier 4 - Low Priority'
    
    def enrich_leads(self):
        """Add calculated fields to leads with both legacy and enhanced scoring"""
        print("Enriching leads with scoring and recommendations...")
        
        # Extract structured fields
        self.combined_df['Sector'] = self.combined_df['Other Details'].apply(self.extract_sector)
        self.combined_df['Category'] = self.combined_df['Other Details'].apply(self.extract_category)
        self.combined_df['Submission_Year'] = self.combined_df['Other Details'].apply(self.extract_submission_year)
        
        # Calculate LEGACY score (existing categorical system)
        print("  Calculating legacy scores...")
        self.combined_df['Lead_Score'] = self.combined_df.apply(self.calculate_lead_score, axis=1)
        
        # Calculate ENHANCED scoring components
        print("  Calculating enhanced scoring components...")
        
        # Component 1: Intent Strength (Weight: 0.4)
        self.combined_df['Intent_Signal'] = self.combined_df['Project_Description'].apply(
            self.calculate_intent_strength
        )
        
        # Component 2: Freshness (Weight: 0.3)
        self.combined_df['Freshness_Score'] = self.combined_df['Other Details'].apply(
            self.calculate_freshness
        )
        
        # Component 3: Company Size Proxy (Weight: 0.2)
        self.combined_df['Size_Score'] = self.combined_df.apply(
            lambda x: self.calculate_size_proxy(x['Project_Description'], x['Category']),
            axis=1
        )
        
        # Component 4: Geographic Proximity (Weight: 0.1)
        self.combined_df['Proximity_Score'] = self.combined_df.apply(
            lambda x: self.calculate_proximity(x['Location'], x['State'], x.get('Project_Description', '')),
            axis=1
        )
        
        # Calculate ENHANCED score (weighted combination)
        print("  Computing enhanced scores...")
        self.combined_df['Enhanced_Score'] = (
            100 * (
                self.combined_df['Intent_Signal'] * 0.4 +
                self.combined_df['Freshness_Score'] * 0.3 +
                self.combined_df['Size_Score'] * 0.2 +
                self.combined_df['Proximity_Score'] * 0.1
            )
        ).round(2)
        
        # Calculate FINAL HYBRID score (30% legacy + 70% enhanced)
        print("  Creating hybrid final score (30% legacy + 70% enhanced)...")
        self.combined_df['Final_Score'] = (
            0.3 * self.combined_df['Lead_Score'] +
            0.7 * self.combined_df['Enhanced_Score']
        ).round(2)
        
        # Add High Urgency Flag
        self.combined_df['High_Urgency_Flag'] = (
            (self.combined_df['Final_Score'] > 70) |
            (self.combined_df['Intent_Signal'] >= 0.9)
        )
        
        # Add product recommendations
        self.combined_df['Recommended_Products'] = self.combined_df['Sector'].apply(self.get_products)
        
        # Update tier assignment using FINAL SCORE
        self.combined_df['Priority_Tier'] = self.combined_df.apply(
            lambda x: self.assign_tier(x['Final_Score'], x['Proposal Status'], x['Submission_Year']), 
            axis=1
        )
        
        print("Enrichment complete!\n")
    
    def export_filtered_leads(self, output_path, min_score=40):
        """Export filtered and sorted leads"""
        
        # Filter by minimum FINAL SCORE (hybrid)
        filtered = self.combined_df[self.combined_df['Final_Score'] >= min_score].copy()
        
        # Sort by Final_Score descending
        filtered = filtered.sort_values('Final_Score', ascending=False)
        
        # Use original ID from Proposal No. instead of sequential
        filtered.insert(0, 'ID', filtered['Proposal No.'])
        
        # Select and reorder columns for agent use (ENHANCED EXPORT)
        export_columns = [
            'ID',                   # MODIFIED: Original Proposal No.
            'Company_Name',         # NEW: From leads_export
            'Signal_Type',          # NEW: From leads_export
            'Source_URL',           # NEW: From leads_export
            'Priority_Tier',
            'Final_Score',          # NEW: Hybrid score (primary)
            'High_Urgency_Flag',    # NEW: Urgency indicator
            'Lead_Score',           # Legacy score (for reference)
            'Enhanced_Score',       # NEW: Enhanced score
            'Intent_Signal',        # NEW: Intent strength
            'Freshness_Score',      # NEW: Time decay
            'Size_Score',           # NEW: Company size proxy
            'Proximity_Score',      # NEW: Geographic proximity
            'State',
            'Proposal No.',
            'Project Name',
            'Project Proponent',
            'Location',
            'Sector',
            'Category',
            'Submission_Year',
            'Proposal Status',
            'Project_Description',
            'Other Details',
            'Recommended_Products' # Keep this from original
        ]
        
        # Ensure all columns exist
        for col in export_columns:
            if col not in filtered.columns:
                filtered[col] = ''
                
        final_df = filtered[export_columns]
        
        # Final Deduplication (Requested by User)
        original_len = len(final_df)
        final_df = final_df.drop_duplicates(subset=['ID'], keep='first')
        if len(final_df) < original_len:
            print(f"  Removed {original_len - len(final_df)} duplicate records during filtering.")
        
        # Export
        final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"EXPORT SUMMARY")
        print("="*60)
        print(f"Total leads exported: {len(final_df)}")
        print(f"\nBy Priority Tier:")
        print(final_df['Priority_Tier'].value_counts())
        print(f"\nBy State:")
        print(final_df['State'].value_counts())
        print(f"\nFinal Score Distribution:")
        print(f"  90-100: {len(final_df[final_df['Final_Score'] >= 90])}")
        print(f"  80-89:  {len(final_df[(final_df['Final_Score'] >= 80) & (final_df['Final_Score'] < 90)])}")
        print(f"  70-79:  {len(final_df[(final_df['Final_Score'] >= 70) & (final_df['Final_Score'] < 80)])}")
        print(f"  60-69:  {len(final_df[(final_df['Final_Score'] >= 60) & (final_df['Final_Score'] < 70)])}")
        print(f"  40-59:  {len(final_df[(final_df['Final_Score'] >= 40) & (final_df['Final_Score'] < 60)])}")
        print(f"\nHigh Urgency Leads: {final_df['High_Urgency_Flag'].sum()}")
        print(f"\nFile saved to: {output_path}")
        print("="*60)
        
        return final_df

def main():
    """Main execution"""
    print("\n" + "="*60)
    print("LEAD FILTERING & EXPORT TOOL")
    print("="*60 + "\n")
    
    # Initialize
    base_path = Path(__file__).parent / 'data'
    filter_tool = LeadFilter(base_path)
    
    # Load all data
    filter_tool.load_all_data()
    
    # Enrich with scores and recommendations
    filter_tool.enrich_leads()
    
    # Export different tiers
    print("\n" + "="*60)
    print("EXPORTING LEADS...")
    print("="*60 + "\n")
    
    # Export all qualified leads (score >= 40)
    output_file = Path('data/filtered_leads_for_agents.csv')
    leads_df = filter_tool.export_filtered_leads(output_file, min_score=40)
    
    # Also export just Tier 1 for immediate action
    tier1_file = Path('data/tier1_immediate_action_leads.csv')
    tier1 = leads_df[leads_df['Priority_Tier'] == 'Tier 1 - Immediate']
    tier1.to_csv(tier1_file, index=False, encoding='utf-8-sig')
    print(f"\nTier 1 leads (immediate action): {len(tier1)} saved to {tier1_file}")

    # Export full filtered dataset (Tier 1, 2, 3) as requested
    full_dataset_file = Path('data/filtered_dataset.csv')
    leads_df.to_csv(full_dataset_file, index=False, encoding='utf-8-sig')
    print(f"Full filtered dataset: {len(leads_df)} saved to {full_dataset_file}")
    
    print("\n" + "="*60)
    print("COMPLETE! Leads ready for agent assignment.")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
