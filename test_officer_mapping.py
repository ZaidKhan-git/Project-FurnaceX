# test_officer_mapping.py - Quick Test for Officer Mapping System
"""
Test the officer mapping system with a small sample of data to verify functionality
before processing the full dataset.
"""

import pandas as pd
from pathlib import Path
from map_officers_to_leads import OfficerMapper


def create_test_dataset():
    """Create a small test dataset from first 10 rows"""
    print("Creating test dataset from first 10 rows...")
    
    # Read full dataset
    df_full = pd.read_csv("data/filtered_dataset.csv")
    
    # Take first 10 rows
    df_test = df_full.head(10).copy()
    
    # Save test dataset
    test_path = Path("data/test_filtered_dataset.csv")
    df_test.to_csv(test_path, index=False)
    
    print(f"  ✓ Created test dataset with {len(df_test)} rows")
    print(f"  Saved to: {test_path}")
    
    return test_path


def test_officer_mapping():
    """Test officer mapping on small dataset"""
    print("\n" + "=" * 70)
    print("TESTING OFFICER MAPPING SYSTEM (10 Rows)")
    print("=" * 70)
    
    # Create test dataset
    test_path = create_test_dataset()
    
    # Initialize mapper with test dataset
    mapper = OfficerMapper(dataset_path=str(test_path))
    
    # Load and map
    mapper.load_dataset()
    mapper.map_officers_to_dataset(save_path="data/test_with_officers.csv")
    
    # Validate
    stats = mapper.validate_mapping()
    
    print("\n" + "=" * 70)
    print("✓ TEST COMPLETED!")
    print("=" * 70)
    print("\nReview the output file: data/test_with_officers.csv")
    print("If the results look good, run: python map_officers_to_leads.py")
    print("to process the full dataset.")
    
    return stats


if __name__ == "__main__":
    test_officer_mapping()
