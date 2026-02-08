"""
Geospatial Routing Module.
Maps leads to sales territories using location inference and geometric predicates.
"""
from shapely.geometry import Point, Polygon
from typing import Optional, Tuple, Dict, Any

class GeospatialRouter:
    
    # Simplified territories (approximate bounding boxes for demo)
    # Format: [(lon, lat), ...]
    TERRITORIES = {
        "North Region": Polygon([(72.0, 37.0), (81.0, 37.0), (81.0, 28.0), (72.0, 28.0)]),  # JK, HP, Punjab, Delhi
        "West Region": Polygon([(68.0, 28.0), (76.0, 28.0), (76.0, 15.0), (68.0, 15.0)]),   # Gujarat, Maharashtra
        "South Region": Polygon([(74.0, 20.0), (85.0, 20.0), (85.0, 8.0), (74.0, 8.0)]),    # KN, TN, AP, KL
        "East Region": Polygon([(82.0, 28.0), (97.0, 28.0), (97.0, 20.0), (82.0, 20.0)]),   # WB, Odisha, Bihar
    }
    
    # State-to-Coordinate mapping (Centroids) for text inference
    STATE_COORDS = {
        "Gujarat": (22.25, 71.19),
        "Maharashtra": (19.75, 75.71),
        "Delhi": (28.70, 77.10),
        "Tamil Nadu": (11.12, 78.65),
        "Karnataka": (15.31, 75.71),
        "West Bengal": (22.98, 87.85),
        "Bihar": (25.09, 85.31),
        "Odisha": (20.95, 85.09),
        "Punjab": (31.14, 75.34),
    }

    def infer_location(self, text: str) -> Optional[Tuple[float, float]]:
        """Extract approximate lat/lon from text based on state names."""
        for state, coords in self.STATE_COORDS.items():
            if state.lower() in text.lower():
                return coords
        return None

    def route_lead(self, lead_text: str, lat: float = None, lon: float = None) -> Dict[str, Any]:
        """
        Route lead to a territory.
        Priority: Provided Lat/Lon > Inferred Loation > Unassigned
        """
        if lat is None or lon is None:
            coords = self.infer_location(lead_text)
            if coords:
                lat, lon = coords
            else:
                return {"territory": "Unassigned", "coordinates": None}
        
        point = Point(lon, lat)
        
        for territory_name, polygon in self.TERRITORIES.items():
            if polygon.contains(point):
                return {
                    "territory": territory_name,
                    "coordinates": (lat, lon),
                    "sales_officer_id": f"SO_{territory_name.split()[0].upper()}_01"
                }
                
        return {"territory": "Out of Territory", "coordinates": (lat, lon)}
