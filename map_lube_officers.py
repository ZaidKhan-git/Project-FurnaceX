"""
map_lube_officers.py - Map Specialty Product Leads to Lube Regional Officers

This script:
1. Reads the full filtered dataset (filtered_dataset.csv)
2. Identifies leads relevant to Specialty Products (Lubes & Specialties)
3. Maps them to the nearest Lube Regional Officer based on location/zone
4. Adds officer details (Name, Email, Phone, Address) as new columns
5. Exports the result to 'specialty_leads_with_officers.csv'
"""

import pandas as pd
import json
import re
from pathlib import Path

# Specialty Products Keywords
SPECIALTY_PRODUCTS = [
    r'Jute Batch Oil', r'JBO',
    r'Mineral Turpentine Oil', r'MTO', r'MTO\s*2445',
    r'Solvent\s*1425',
    r'Hexane',
    r'Propylene',
    r'Sulphur', r'Molten Sulphur',
    r'Bitumen',
    r'Marine Bunker Fuel', r'Bunker Fuel', r'Marine Fuel'
]

# State to Zone/Office Mapping (approximate based on Lube RO locations)
STATE_OFFICE_MAP = {
    'Maharashtra': ['Mumbai', 'Pune', 'Nagpur'], # Multiple offices
    'Madhya Pradesh': ['Bhopal', 'Indore'],      # Multiple offices
    'Gujarat': ['Mumbai'], # West Zone fallback
    'Goa': ['Mumbai'],
    'Chhattisgarh': ['Nagpur'],
    'Uttar Pradesh': ['Lucknow', 'Agra'], # Assuming Lube presence or fallback to Delhi
    'West Bengal': ['Kolkata'],
    'Odisha': ['Kolkata'],
    'Bihar': ['Kolkata'],
    'Jharkhand': ['Kolkata'],
    'Assam': ['Kolkata'],
    'Delhi': ['Delhi/NCR'],
    'Haryana': ['Delhi/NCR'],
    'Punjab': ['Delhi/NCR'],
    'Rajasthan': ['Delhi/NCR'], # Or Jaipur if exists
    'Tamil Nadu': ['Chennai'],
    'Karnataka': ['Chennai'], # Or Bangalore
    'Kerala': ['Chennai'], # Or Kochi
    'Andhra Pradesh': ['Chennai'], # Or Vizag
    'Telangana': ['Chennai'] # Or Hyderabad
}

def load_lube_officers():
    try:
        with open('config/lube_regional_officer.json', 'r') as f:
            data = json.load(f)
            return data.get('lube_officers', [])
    except FileNotFoundError:
        print("Error: config/lube_regional_officers.json not found.")
        return []

def get_specialty_product_match(description):
    """Check if lead description matches any specialty product"""
    if pd.isna(description):
        return None
    
    matches = []
    for pattern in SPECIALTY_PRODUCTS:
        if re.search(pattern, str(description), re.IGNORECASE):
            matches.append(pattern.replace(r'\s*', ' ').replace(r'\\b', ''))
    
    return ", ".join(list(set(matches))) if matches else None

def find_nearest_lube_officer(state, city, officers):
    """Find the most appropriate Lube RO based on state and city"""
    
    # Simple logic: Check explicit state mapping first
    target_offices = STATE_OFFICE_MAP.get(state, [])
    
    if not target_offices:
        return None # No mapping found
    
    # If multiple offices (e.g. MH has Mumbai, Pune, Nagpur), try to match city/district
    selected_office_location = target_offices[0] # Default to first
    
    if len(target_offices) > 1 and city:
        city_lower = str(city).lower()
        # Heuristic: if city name matches office location, pick that
        for office_loc in target_offices:
            if office_loc.lower() in city_lower:
                selected_office_location = office_loc
                break
        
        # Additional heuristics could be added here (distance based)
    
    # Find the officer entry for this location
    for officer in officers:
        if officer['location'] == selected_office_location:
            return officer
            
    # Fallback to first if exact location match fails but state mapped
    for officer in officers:
         if officer['location'] in target_offices:
             return officer
             
    return None

def main():
    print("Mapping Specialty Leads to Lube Officers...")
    
    # 1. Load Data
    try:
        df = pd.read_csv('data/filtered_dataset.csv')
    except FileNotFoundError:
        print("Error: data/filtered_dataset.csv not found. Run filter_leads.py first.")
        return

    lube_officers = load_lube_officers()
    if not lube_officers:
        return

    # 2. Identify Specialty Leads
    print("Identifying specialty product leads...")
    df['Specialty_Products_Found'] = df['Project_Description'].apply(get_specialty_product_match)
    
    # 3. Map Officers
    print("Mapping officers...")
    
    def map_row(row):
        matches = row['Specialty_Products_Found']
        if not matches:
             return pd.Series([None, None, None, None]) # No conversion if not specialty
             
        officer = find_nearest_lube_officer(row.get('State'), row.get('Location'), lube_officers)
        
        if officer:
            return pd.Series([officer.get('name'), officer.get('email'), officer.get('phone'), officer.get('address')])
        else:
            return pd.Series(["Unassigned", "lubescare@hpcl.in", "1800-2333-555", "Please contact HQ"])

    officer_cols = ['Lube_Officer_Name', 'Lube_Officer_Email', 'Lube_Officer_Phone', 'Lube_Officer_Address']
    df[officer_cols] = df.apply(map_row, axis=1)
    
    # Filter to only showing leads that have matched products (optional, or just enrich all)
    # The user said "only map the officer... with the leads that have heavy weights in above mentioned products"
    # So we should probably allow the user to see the full list or just the matched ones.
    # Let's create a subset for the "Specialty Team"
    
    specialty_leads = df[df['Specialty_Products_Found'].notna()].copy()
    
    if not specialty_leads.empty:
        output_file = Path('data/lube_specialty_leads.csv')
        specialty_leads.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"Found {len(specialty_leads)} specialty product leads.")
        print(f"Exported to {output_file}")
        
        # Display sample
        print("\nSample Mapped Lead:")
        sample = specialty_leads.iloc[0]
        # Try multiple variations for Project Name
        project_name = sample.get('Project Name', sample.get('Project_Name', 'Unknown Project'))
        print(f"Project: {project_name}")
        print(f"Product Match: {sample['Specialty_Products_Found']}")
        print(f"Officer: {sample['Lube_Officer_Name']} ({sample['Lube_Officer_Email']})")
    else:
        print("No specialty product leads found with current keywords.")

if __name__ == "__main__":
    main()
