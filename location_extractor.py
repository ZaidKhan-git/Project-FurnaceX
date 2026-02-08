"""
location_extractor.py - Extract District/City from Lead Data

Parses Project_Description, Location, and other fields to identify
the actual district or city for geocoding purposes.
"""

import re

def extract_district_city(location_str, project_desc, state):
    """
    Extract district/city name from location string or project description
    
    Returns: Extracted location name (district/city) or state as fallback
    """
    if not location_str and not project_desc:
        return state  # Fallback to state
    
    combined_text = f"{location_str} {project_desc}".lower()
    
    # Common patterns for district/city extraction
    patterns = [
        r'district[:\-\s]+([a-z\s]+)',  # "District: Purulia" or "District- Purulia"
        r'dist[\.\-\s]+([a-z\s]+)',      # "Dist. Purulia" or "Dist- Purulia"
        r'district[:\-\s]+([a-z\s]+)',   # "district: Purulia"
        r'taluka[:\-\s]+([a-z\s]+)',     # "Taluka: Shirur"
        r'tal[\.\-\s]+([a-z\s]+)',       # "Tal. Shirur"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, combined_text, re.IGNORECASE)
        if match:
            extracted = match.group(1).strip().title()
            # Clean up common suffixes
            extracted = re.sub(r'\s+(and|,|\.|\().*$', '', extracted).strip()
            if len(extracted) > 3:  # Must be at least 4 characters
                return extracted
    
    # If no pattern matched, return state as fallback
    return state


if __name__ == "__main__":
    # Test cases
    test_cases = [
        ("WEST BENGAL", "District- Purulia, West Bengal", "West Bengal"),
        ("MAHARASHTRA", "Taluka Shirur, Dist. Pune", "Maharashtra"),
        ("MADHYA PRADESH", "District Satna, Madhya Pradesh", "Madhya Pradesh"),
    ]
    
    for loc, desc, state in test_cases:
        result = extract_district_city(loc, desc, state)
        print(f"Location: {loc}, Description: {desc[:50]}...")
        print(f"  → Extracted: {result}\n")
