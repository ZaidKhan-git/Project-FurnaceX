# geocode_proximity.py - Real Geographic Proximity Scoring with Geocoding
"""
This module implements true distance-based proximity scoring using:
1. Manual geocoding (city/district → lat/lon mapping from existing data)
2. Haversine distance calculation to nearest HPCL depot
3. Distance-based scoring (closer = higher score)

NO APIs needed - uses pre-compiled lat/lon data for Indian cities/districts.
"""

import json
import pandas as pd
from math import radians, cos, sin, asin, sqrt
from pathlib import Path

class GeographicProximityScorer:
    def __init__(self):
        # HPCL Depot coordinates (manually compiled from Google Maps)
        self.hpcl_depots = {
            # Maharashtra
            'Mumbai': (19.0760, 72.8777),
            'Nagpur': (21.1458, 79.0882),
            'Pune': (18.5204, 73.8567),
            
            # Madhya Pradesh  
            'Bhopal': (23.2599, 77.4126),
            'Indore': (22.7196, 75.8577),
            
            # Uttar Pradesh
            'Lucknow': (26.8467, 80.9462),
            'Kanpur': (26.4499, 80.3319),
            'Agra': (27.1767, 78.0081),
            'Mathura': (27.4924, 77.6737),
            'Varanasi': (25.3176, 82.9739),
           
            'Kolkata': (22.5726, 88.3639),
            
            # Haryana
            'Panipat': (29.3909, 76.9635),
            'Gurugram': (28.4595, 77.0266),
            'Rewari': (28.1989, 76.6191),
        }
        
        # District/City coordinates for leads (Indian major locations)
        self.location_coords = self._load_location_database()
    
    def _load_location_database(self):
        """Load pre-compiled coordinates for Indian districts/cities"""
        # This is a simplified database - in production, use comprehensive dataset
        coords = {
            # Maharashtra
            'Purulia': (23.3387, 86.3660),  # West Bengal actually
            'Thane': (19.2183, 72.9781),
            'Mumbai': (19.0760, 72.8777),
            'Pune': (18.5204, 73.8567),
            'Nagpur': (21.1458, 79.0882),
            'Aurangabad': (19.8762, 75.3433),
            'Buldhana': (20.5311, 76.1837),
            'Satara': (17.6859, 74.0183),
            'Raig': (18.3357, 73.5179),
            'Palghar': (19.6966, 72.7657),
            'Sangli': (16.8524, 74.5815),
            'Kolhapur': (16.7050, 74.2433),
            'Nashik': (19.9975, 73.7898),
            'Solapur': (17.6599, 75.9064),
            'Latur': (18.4088,76.5604),
            'Parbhani': (19.2608, 76.7794),
            'Chandrapur': (19.9615, 79.2961),
            
            # Madhya Pradesh
            'Satna': (24.6005, 80.8322),
            'Neemuch': (24.4709, 74.8663),
            'Sehore': (23.2004, 77.0833),
            'Dhar': (22.5990, 75.2974),
            'Seoni': (22.0852, 79.5498),
            'Chhatarpur': (24.9177, 79.5890),
            'Katni': (23.8346, 80.3973),
            'Ujjain': (23.1765, 75.7885),
            'Indore': (22.7196, 75.8577),
            'Bhopal': (23.2599, 77.4126),
            'Panna': (24.7161, 80.1947),
            'Shivpuri': (25.4213, 77.6605),
            'Sidhi': (24.4148, 81.8828),
            'Barwani': (22.0322, 74.9022),
            'Anuppur': (23.1041, 81.6893),
            'Betul': (21.9079, 77.8987),
            'Mandla': (22.5990, 80.3712),
            'Damoh': (23.8333, 79.4333),
            'Shajapur': (23.4262, 76.2737),
            'Sagar': (23.8388, 78.7378),
            
            # Uttar Pradesh
            'Ghaziabad': (28.6692, 77.4538),
            'Lucknow': (26.8467, 80.9462),
            'Kanpur': (26.4499, 80.3319),
            'Mathura': (27.4924, 77.6737),
            'Gurugram': (28.4595, 77.0266),
            'Prayagraj': (25.4358, 81.8463),
            'Sitapur': (27.5672, 80.6833),
            'Sonbhadra': (24.6889, 83.0667),
            'Shamli': (29.4499, 77.3131),
            'Muzaffarnagar': (29.4707, 77.7033),
            'Saharanpur': (29.9680, 77.5478),
            'Bijnor': (29.3729,78.1369),
            'Moradabad': (28.8389, 78.7764),
            'Rampur': (28.8154, 79.0254),
            'Bareilly': (28.3670, 79.4304),
            'Jhansi': (25.4484, 78.5685),
            'Hamirpur': (25.9518, 80.1480),
            'Banda': (25.4765, 80.3359),
            'Jalaun': (26.1452, 79.3338),
            'Mirzapur': (25.1460, 82.5651),
            'Ayodhya': (26.7986, 82.1996),
            'Unnao': (26.5472, 80.4937),
            'Sambhal': (28.5854, 78.5628),
            'Hapur': (28.7215, 77.7624),
            'Gorakhpur': (26.7594, 83.3636),
            'Varanasi': (25.3176, 82.9739),
            'Gonda': (27.1385, 81.9565),
            
            # West Bengal
            'Purulia': (23.3387, 86.3660),
            'Kolkata': (22.5726, 88.3639),
            'East Midnapur': (22.2842, 87.7896),
            'North 24 Parganas': (22.6160, 88.4011),
            'Paschim Bardhaman': (23.8250, 87.7120),
            'Birbhum': (23.8400, 87.6198),
            'Bankura': (23.2324, 87.0714),
            
            # Haryana
            'Gurugram': (28.4595, 77.0266),
            'Panipat': (29.3909, 76.9635),
            'Rewari': (28.1989, 76.6191),
            'Karnal': (29.6857, 76.9905),
        }
        return coords
    
    def haversine_distance(self, lat1, lon1, lat2, lon2):
        """
        Calculate great circle distance between two points on Earth (in km)
        Using Haversine formula
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
    
    def geocode_location(self, location_str, state, project_desc=""):
        """
        Extract coordinates from location string and project description.
        Uses pre-compiled coordinates for Indian districts/cities.
        Falls back to state capitals when location is unknown.
        
        NOTE: Nominatim API (free, no key) is available but disabled for speed.
        Enable by uncommenting the Nominatim block below for dynamic geocoding.
        """
        from location_extractor import extract_district_city
        
        # Extract district/city from description
        district = extract_district_city(location_str, project_desc, state)
        
        if pd.isna(district) or not district:
            district = state  # Fallback to state
        
        district_clean = str(district).strip().title()
        
        # Try direct lookup in pre-compiled coords (fast path)
        if district_clean in self.location_coords:
            return self.location_coords[district_clean]
        
        # Try partial match (e.g., "East Midnapur" matches "Midnapur")
        for city, coords in self.location_coords.items():
            if city.lower() in district_clean.lower() or district_clean.lower() in city.lower():
                return coords
        
        # OPTIONAL: Enable Nominatim for accurate geocoding of unknown locations
        # Uncomment below to use free OpenStreetMap API (1 req/sec rate limit)
        # try:
        #     from nominatim_geocoder import NominatimGeocoder
        #     if not hasattr(self, 'nominatim'):
        #         self.nominatim = NominatimGeocoder()
        #     coords = self.nominatim.geocode(district_clean, state)
        #     if coords:
        #         self.location_coords[district_clean] = coords
        #         return coords
        # except Exception:
        #     pass
        
        # Fallback to state capitals
        state_capitals = {
            'Maharashtra': (19.0760, 72.8777),  # Mumbai
            'Madhya Pradesh': (23.2599, 77.4126),  # Bhopal
            'Uttar Pradesh': (26.8467, 80.9462),  # Lucknow
            'West Bengal': (22.5726, 88.3639),  # Kolkata
            'Haryana': (28.4595, 77.0266),  # Gurugram/Delhi vicinity
        }
        
        return state_capitals.get(state, None)
    
    def find_nearest_depot(self, lead_coords):
        """Find nearest HPCL depot and return distance in km"""
        if not lead_coords:
            return float('inf')
        
        lat1, lon1 = lead_coords
        min_distance = float('inf')
        
        for depot_name, (lat2, lon2) in self.hpcl_depots.items():
            distance = self.haversine_distance(lat1, lon1, lat2, lon2)
            if distance < min_distance:
                min_distance = distance
        
        return min_distance
    
    def calculate_proximity_score(self, location, state, project_desc=""):
        """
        Calculate proximity score (0-1) based on distance to nearest depot
        
        Scoring logic:
        - <50 km: 1.0 (excellent proximity)
        - 50-100 km: 0.9 (very good)
        - 100-200 km: 0.8 (good)
        - 200-300 km: 0.7 (moderate)
        - 300-500 km: 0.6 (distant)
        500+ km: 0.5 (very distant)
        """
        # Geocode lead location
        lead_coords = self.geocode_location(location, state, project_desc)
        
        if not lead_coords:
            return 0.5  # Default for ungeocodable locations
        
        # Find distance to nearest depot
        distance = self.find_nearest_depot(lead_coords)
        
        # Score based on distance thresholds
        if distance < 50:
            return 1.0
        elif distance < 100:
            return 0.9
        elif distance < 200:
            return 0.8
        elif distance < 300:
            return 0.7
        elif distance < 500:
            return 0.6
        else:
            return 0.5
    
    def get_nearest_depot_info(self, location, state, project_desc=""):
        """Get detailed info about nearest depot (for transparency)"""
        lead_coords = self.geocode_location(location, state, project_desc)
        
        if not lead_coords:
            return None
        
        lat1, lon1 = lead_coords
        min_distance = float('inf')
        nearest_depot = None
        
        for depot_name, (lat2, lon2) in self.hpcl_depots.items():
            distance = self.haversine_distance(lat1, lon1, lat2, lon2)
            if distance < min_distance:
                min_distance = distance
                nearest_depot = depot_name
        
        return {
            'nearest_depot': nearest_depot,
            'distance_km': round(min_distance, 1),
            'lead_coords': lead_coords
        }


if __name__ == "__main__":
    # Test the scorer
    scorer = GeographicProximityScorer()
    
    # Example lead from West Bengal (Purulia)
    test_location = "District- Purulia, West Bengal"
    test_state = "West Bengal"
    
    score = scorer.calculate_proximity_score(test_location, test_state)
    info = scorer.get_nearest_depot_info(test_location, test_state)
    
    print(f"Location: {test_location}")
    print(f"Proximity Score: {score}")
    print(f"Nearest Depot: {info['nearest_depot']}")
    print(f"Distance: {info['distance_km']} km")
    print(f"Coordinates: {info['lead_coords']}")
