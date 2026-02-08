# nominatim_geocoder.py - Free Geocoding using OpenStreetMap Nominatim
"""
Accurate geocoding using OpenStreetMap's Nominatim service.
NO API KEY REQUIRED - completely free.

Rate limit: 1 request/second (we'll add caching to minimize calls)
"""

import urllib.request
import urllib.parse
import json
import time
from pathlib import Path

class NominatimGeocoder:
    """Free geocoding using OpenStreetMap Nominatim API"""
    
    BASE_URL = "https://nominatim.openstreetmap.org/search"
    CACHE_FILE = Path("data/geocode_cache.json")
    
    def __init__(self):
        self.cache = self._load_cache()
        self.last_request = 0
    
    def _load_cache(self):
        """Load cached geocoding results"""
        try:
            if self.CACHE_FILE.exists():
                with open(self.CACHE_FILE, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {}
    
    def _save_cache(self):
        """Save cache to disk"""
        try:
            self.CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(self.CACHE_FILE, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save cache: {e}")
    
    def _rate_limit(self):
        """Respect Nominatim rate limit (1 req/sec)"""
        elapsed = time.time() - self.last_request
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        self.last_request = time.time()
    
    def geocode(self, location_name, state="India"):
        """
        Geocode a location name to lat/lon coordinates.
        
        Args:
            location_name: City, district, or place name
            state: State name for disambiguation
            
        Returns:
            (latitude, longitude) tuple or None if not found
        """
        # Check cache first
        cache_key = f"{location_name}, {state}"
        if cache_key in self.cache:
            return tuple(self.cache[cache_key]) if self.cache[cache_key] else None
        
        # Build query
        query = f"{location_name}, {state}, India"
        params = urllib.parse.urlencode({
            'q': query,
            'format': 'json',
            'limit': 1,
            'addressdetails': 0
        })
        
        url = f"{self.BASE_URL}?{params}"
        
        try:
            self._rate_limit()
            
            req = urllib.request.Request(
                url,
                headers={'User-Agent': 'PetroleumIntel/1.0 (lead-scoring-system)'}
            )
            
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())
            
            if data and len(data) > 0:
                lat = float(data[0]['lat'])
                lon = float(data[0]['lon'])
                coords = (lat, lon)
                self.cache[cache_key] = list(coords)
                self._save_cache()
                return coords
            else:
                self.cache[cache_key] = None
                self._save_cache()
                return None
                
        except Exception as e:
            print(f"  Geocoding error for {location_name}: {e}")
            return None


if __name__ == "__main__":
    geocoder = NominatimGeocoder()
    
    # Test with Indian locations
    test_locations = [
        ("Purulia", "West Bengal"),
        ("Thane", "Maharashtra"),
        ("Satna", "Madhya Pradesh"),
        ("Lucknow", "Uttar Pradesh"),
        ("Gurugram", "Haryana"),
    ]
    
    print("Testing Nominatim Geocoder (NO API KEY NEEDED):")
    print("=" * 60)
    
    for location, state in test_locations:
        coords = geocoder.geocode(location, state)
        print(f"{location}, {state} → {coords}")
    
    print("\n✓ Geocoding completed without any API key!")
