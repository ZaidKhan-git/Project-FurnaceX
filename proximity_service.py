# proximity_service.py - Industrial-Grade Proximity Service for HPCL Officer Mapping
"""
Professional proximity matching service to assign nearest HPCL officers based on
geographical distance calculations.

Features:
- Haversine distance calculations
- Geocoding with caching
- State-based fallback logic
- Rate limiting compliance
- Batch processing capability
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from math import radians, cos, sin, asin, sqrt
import time

class ProximityService:
    """Service for finding nearest HPCL officers to project locations"""
    
    def __init__(self, officers_file: str = "config/hpcl_officers.json"):
        """Initialize proximity service with officer data"""
        self.officers = self._load_officers(officers_file)
        self.geocode_cache = {}
        self.last_geocode_request = 0
        
        # Initialize geocoder (lazy loading)
        self._geocoder = None
        
    def _load_officers(self, filepath: str) -> List[Dict]:
        """Load HPCL officers from JSON file"""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('depots', [])
    
    def _get_geocoder(self):
        """Lazy load geocoder with pre-compiled database"""
        if self._geocoder is None:
            from geocode_proximity import GeographicProximityScorer
            self._geocoder = GeographicProximityScorer()
        return self._geocoder
    
    def haversine_distance(self, lat1: float, lon1: float, 
                          lat2: float, lon2: float) -> float:
        """
        Calculate great circle distance between two points using Haversine formula
        
        Args:
            lat1, lon1: First point coordinates
            lat2, lon2: Second point coordinates
            
        Returns:
            Distance in kilometers
        """
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        
        # Earth radius in kilometers
        r = 6371
        return c * r
    
    def geocode_location(self, location: str, state: str) -> Optional[Tuple[float, float]]:
        """
        Geocode a location string to coordinates
        
        Args:
            location: City/district name
            state: State name
            
        Returns:
            (latitude, longitude) tuple or None
        """
        # Check cache first
        cache_key = f"{location}, {state}"
        if cache_key in self.geocode_cache:
            return self.geocode_cache[cache_key]
        
        # Use pre-compiled geocoding database (fast, offline)
        try:
            geocoder = self._get_geocoder()
            coords = geocoder.geocode_location(location, state, "")
            self.geocode_cache[cache_key] = coords
            return coords
        except Exception as e:
            print(f"  Warning: Geocoding failed for {location}, {state}: {e}")
            return None
    
    def geocode_officer_location(self, officer: Dict) -> Optional[Tuple[float, float]]:
        """
        Geocode officer location from their data
        
        Args:
            officer: Officer dictionary with location/state/address
            
        Returns:
            (latitude, longitude) tuple or None
        """
        location = officer.get('location', '')
        state = officer.get('state', '')
        
        if not location or not state:
            return None
        
        return self.geocode_location(location, state)
    
    def find_nearest_officer(self, 
                            project_location: str, 
                            project_state: str,
                            max_radius_km: float = 500) -> Optional[Dict]:
        """
        Find nearest HPCL officer to a project location
        
        Args:
            project_location: Project city/district
            project_state: Project state
            max_radius_km: Maximum search radius in kilometers
            
        Returns:
            Dictionary with officer info and distance, or None
        """
        # Geocode project location
        project_coords = self.geocode_location(project_location, project_state)
        
        if not project_coords:
            # Fallback: find officer in same state
            return self._find_officer_by_state(project_state)
        
        project_lat, project_lon = project_coords
        
        # Find nearest officer
        min_distance = float('inf')
        nearest_officer = None
        
        for officer in self.officers:
            officer_coords = self.geocode_officer_location(officer)
            
            if not officer_coords:
                continue
            
            officer_lat, officer_lon = officer_coords
            distance = self.haversine_distance(
                project_lat, project_lon,
                officer_lat, officer_lon
            )
            
            if distance < min_distance:
                min_distance = distance
                nearest_officer = officer
        
        if nearest_officer and min_distance <= max_radius_km:
            # Use descriptive name if actual name is empty
            officer_name = nearest_officer.get('name', '').strip()
            if not officer_name or officer_name == 'N/A':
                # Create descriptive identifier: Role @ Location
                role = nearest_officer.get('role', 'Officer')
                location = nearest_officer.get('location', 'HPCL')
                officer_name = f"{role} - {location}"
            
            return {
                'officer_name': officer_name,
                'officer_phone': nearest_officer.get('phone', 'N/A'),
                'officer_email': nearest_officer.get('email', 'N/A'),
                'officer_address': nearest_officer.get('address', 'N/A'),
                'officer_role': nearest_officer.get('role', 'N/A'),
                'distance_km': round(min_distance, 2)
            }
        
        # Fallback to state-based matching
        return self._find_officer_by_state(project_state)
    
    def _find_officer_by_state(self, state: str) -> Optional[Dict]:
        """
        Fallback: Find any officer in the same state
        
        Args:
            state: State name
            
        Returns:
            Dict with officer info or None
        """
        for officer in self.officers:
            if officer.get('state', '').lower() == state.lower():
                # Use descriptive name if actual name is empty
                officer_name = officer.get('name', '').strip()
                if not officer_name or officer_name == 'N/A':
                    role = officer.get('role', 'Officer')
                    location = officer.get('location', 'HPCL')
                    officer_name = f"{role} - {location}"
                
                return {
                    'officer_name': officer_name,
                    'officer_phone': officer.get('phone', 'N/A'),
                    'officer_email': officer.get('email', 'N/A'),
                    'officer_address': officer.get('address', 'N/A'),
                    'officer_role': officer.get('role', 'N/A'),
                    'distance_km': None  # Unknown distance for state fallback
                }
        
        # No officer found in state
        return {
            'officer_name': 'Contact Head Office',
            'officer_phone': 'N/A',
            'officer_email': 'info@hpcl.in',
            'officer_address': 'HPCL Head Office',
            'officer_role': 'General Contact',
            'distance_km': None
        }
    
    def batch_find_nearest_officers(self, 
                                   locations: List[Tuple[str, str]],
                                   progress_callback=None) -> List[Optional[Dict]]:
        """
        Batch process multiple locations to find nearest officers
        
        Args:
            locations: List of (location, state) tuples
            progress_callback: Optional callback for progress updates
            
        Returns:
            List of officer dictionaries
        """
        results = []
        total = len(locations)
        
        for i, (location, state) in enumerate(locations, 1):
            officer = self.find_nearest_officer(location, state)
            results.append(officer)
            
            if progress_callback and i % 10 == 0:
                progress_callback(i, total)
        
        return results


if __name__ == "__main__":
    # Test the proximity service
    print("Testing Proximity Service")
    print("=" * 60)
    
    service = ProximityService()
    
    # Test cases
    test_cases = [
        ("Purulia", "West Bengal"),
        ("Thane", "Maharashtra"),
        ("Satna", "Madhya Pradesh"),
        ("Lucknow", "Uttar Pradesh"),
        ("Gurugram", "Haryana"),
    ]
    
    for location, state in test_cases:
        print(f"\nFinding officer for: {location}, {state}")
        result = service.find_nearest_officer(location, state)
        
        if result:
            print(f"  Officer: {result['officer_name']}")
            print(f"  Role: {result['officer_role']}")
            print(f"  Phone: {result['officer_phone']}")
            print(f"  Email: {result['officer_email']}")
            if result['distance_km']:
                print(f"  Distance: {result['distance_km']} km")
        else:
            print("  No officer found")
    
    print("\n✓ Proximity service testing completed!")
