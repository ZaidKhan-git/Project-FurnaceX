# map_officers_to_leads.py - Map Nearest HPCL Officers to Filtered Dataset
"""
Industrial-grade script to automatically populate HPCL officer contact information
in filtered_dataset.csv based on geographical proximity.

This script:
1. Reads the filtered dataset CSV
2. Extracts location information from each lead
3. Finds the nearest HPCL officer for each lead
4. Adds officer contact columns to the CSV
5. Preserves all existing scores and values

Usage:
    python map_officers_to_leads.py
"""

import pandas as pd
from pathlib import Path
from typing import Optional
import sys

from proximity_service import ProximityService
from location_extractor import extract_district_city


class OfficerMapper:
    """Maps HPCL officers to leads based on proximity"""
    
    def __init__(self, 
                 dataset_path: str = "data/filtered_dataset.csv",
                 officers_file: str = "config/hpcl_officers.json"):
        """
        Initialize the officer mapper
        
        Args:
            dataset_path: Path to filtered dataset CSV
            officers_file: Path to HPCL officers JSON
        """
        self.dataset_path = Path(dataset_path)
        self.proximity_service = ProximityService(officers_file)
        self.df = None
        
    def load_dataset(self) -> pd.DataFrame:
        """Load the filtered dataset"""
        print(f"Loading dataset from {self.dataset_path}...")
        self.df = pd.read_csv(self.dataset_path)
        print(f"  ✓ Loaded {len(self.df)} leads")
        return self.df
    
    def extract_location_from_row(self, row: pd.Series) -> tuple[str, str]:
        """
        Extract location and state from a dataset row
        
        Args:
            row: DataFrame row
            
        Returns:
            (location, state) tuple
        """
        # Get state (should always be present)
        state = str(row.get('State', '')).strip()
        
        # Try to extract more specific location from Location column
        location_field = str(row.get('Location', '')).strip()
        
        # Try to extract district/city from project description if location is not specific
        if location_field and location_field.lower() != state.lower():
            location = location_field
        else:
            # Extract from project name or description
            project_desc = str(row.get('Project_Description', '')) + " " + str(row.get('Project Name', ''))
            location = extract_district_city(location_field, project_desc, state)
            
            # Fallback to state if no specific location found
            if not location or pd.isna(location):
                location = state
        
        return location, state
    
    def map_officers_to_dataset(self, save_path: Optional[str] = None) -> pd.DataFrame:
        """
        Map nearest officers to all leads in dataset
        
        Args:
            save_path: Optional path to save updated CSV (defaults to overwriting original)
            
        Returns:
            Updated DataFrame with officer columns
        """
        if self.df is None:
            self.load_dataset()
        
        print("\nMapping nearest HPCL officers to leads...")
        print("=" * 70)
        
        # Initialize new columns
        self.df['Officer_Name'] = ''
        self.df['Officer_Phone'] = ''
        self.df['Officer_Email'] = ''
        self.df['Officer_Address'] = ''
        self.df['Officer_Role'] = ''
        self.df['Officer_Distance_KM'] = None
        
        total = len(self.df)
        successful = 0
        failed = 0
        
        for idx, row in self.df.iterrows():
            # Progress indicator
            if (idx + 1) % 10 == 0 or (idx + 1) == total:
                print(f"  Processing: {idx + 1}/{total} leads...", end='\r')
            
            try:
                # Extract location
                location, state = self.extract_location_from_row(row)
                
                if not state:
                    print(f"\n  Warning: No state found for row {idx}, skipping...")
                    failed += 1
                    continue
                
                # Find nearest officer
                officer_info = self.proximity_service.find_nearest_officer(location, state)
                
                if officer_info:
                    # Update row with officer information
                    self.df.at[idx, 'Officer_Name'] = officer_info.get('officer_name', 'N/A')
                    self.df.at[idx, 'Officer_Phone'] = officer_info.get('officer_phone', 'N/A')
                    self.df.at[idx, 'Officer_Email'] = officer_info.get('officer_email', 'N/A')
                    self.df.at[idx, 'Officer_Address'] = officer_info.get('officer_address', 'N/A')
                    self.df.at[idx, 'Officer_Role'] = officer_info.get('officer_role', 'N/A')
                    
                    distance = officer_info.get('distance_km')
                    if distance is not None:
                        self.df.at[idx, 'Officer_Distance_KM'] = distance
                    
                    successful += 1
                else:
                    failed += 1
                    
            except Exception as e:
                print(f"\n  Error processing row {idx}: {e}")
                failed += 1
                continue
        
        print(f"\n\n✓ Mapping completed!")
        print(f"  Successfully mapped: {successful} leads")
        print(f"  Failed to map: {failed} leads")
        
        # Save updated dataset
        output_path = save_path if save_path else self.dataset_path
        self.save_dataset(output_path)
        
        return self.df
    
    def save_dataset(self, output_path: str):
        """Save updated dataset to CSV"""
        print(f"\nSaving updated dataset to {output_path}...")
        self.df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"  ✓ Saved successfully!")
        print(f"  New columns added: Officer_Name, Officer_Phone, Officer_Email,")
        print(f"                     Officer_Address, Officer_Role, Officer_Distance_KM")
    
    def validate_mapping(self) -> dict:
        """
        Validate the officer mapping quality
        
        Returns:
            Dictionary with validation statistics
        """
        if self.df is None:
            raise ValueError("No dataset loaded. Call load_dataset() first.")
        
        stats = {
            'total_leads': len(self.df),
            'mapped_with_name': (self.df['Officer_Name'] != '').sum(),
            'mapped_with_name': (self.df['Officer_Name'].notna() & (self.df['Officer_Name'] != 'N/A')).sum(),
            'mapped_with_distance': self.df['Officer_Distance_KM'].notna().sum(),
            'unique_officers': self.df['Officer_Email'].nunique(),
            'states_covered': self.df['State'].nunique(),
        }
        
        print("\n" + "=" * 70)
        print("VALIDATION REPORT")
        print("=" * 70)
        print(f"Total leads: {stats['total_leads']}")
        print(f"Leads with officer assigned: {stats['mapped_with_name']} ({stats['mapped_with_name']/stats['total_leads']*100:.1f}%)")
        print(f"Leads with distance calculated: {stats['mapped_with_distance']} ({stats['mapped_with_distance']/stats['total_leads']*100:.1f}%)")
        print(f"Unique officers assigned: {stats['unique_officers']}")
        print(f"States covered: {stats['states_covered']}")
        
        # Show sample of mapped data
        print("\nSample of mapped data:")
        sample_cols = ['State', 'Location', 'Officer_Name', 'Officer_Phone', 'Officer_Email', 'Officer_Distance_KM']
        available_cols = [col for col in sample_cols if col in self.df.columns]
        print(self.df[available_cols].head(5).to_string(index=False))
        
        return stats


def main():
    """Main execution"""
    print("=" * 70)
    print("HPCL OFFICER PROXIMITY MAPPING SYSTEM")
    print("=" * 70)
    print("This script will map the nearest HPCL officer to each lead in your dataset.")
    print("All existing scores and values will be preserved.")
    print()
    
    # Initialize mapper
    mapper = OfficerMapper()
    
    # Check if dataset exists
    if not mapper.dataset_path.exists():
        print(f"❌ Error: Dataset not found at {mapper.dataset_path}")
        print("   Please run filter_leads.py first to generate the filtered dataset.")
        return 1
    
    # Load dataset
    try:
        mapper.load_dataset()
    except Exception as e:
        print(f"❌ Error loading dataset: {e}")
        return 1
    
    # Map officers
    try:
        mapper.map_officers_to_dataset()
    except Exception as e:
        print(f"❌ Error mapping officers: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Validate results
    try:
        mapper.validate_mapping()
    except Exception as e:
        print(f"⚠️  Warning: Validation failed: {e}")
    
    print("\n" + "=" * 70)
    print("✓ OFFICER MAPPING COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print("\nYour filtered_dataset.csv now includes officer contact information.")
    print("You can use this data to build your automated WhatsApp notification system.")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
