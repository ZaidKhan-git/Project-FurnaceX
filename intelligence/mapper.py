"""
Product-to-Industry Mapper - Maps signals to petroleum products.
"""

import logging
from typing import Dict, List, Any, Set

import sys
sys.path.append(str(__file__).rsplit('\\', 2)[0])
from config.settings import PRODUCT_MAP, KEYWORDS

logger = logging.getLogger(__name__)


class ProductMapper:
    """Maps industrial signals to petroleum product opportunities."""
    
    # Sector inference from keywords
    SECTOR_MAP = {
        "Textiles": ["Stenter Machine", "Jute", "Textile"],
        "Roads": ["NHAI", "PWD", "Highway", "Bitumen", "Asphalt", "Road"],
        "Power": ["Captive Power Plant", "DG Set", "Power Plant", "KVA"],
        "Metals": ["Furnace", "Rolling Mill", "Galvanizing", "Steel"],
        "Mining": ["Mining", "Excavator", "Earth Mover"],
        "Chemicals": ["Solvent", "Extraction", "Hexane"],
        "Shipping": ["Vessel", "Port", "Marine", "Bunker"],
        "Manufacturing": ["Boiler", "Heater", "Industrial"],
    }
    
    def __init__(self):
        self.product_map = PRODUCT_MAP
    
    def infer_products(self, text: str, keywords_matched: Dict[str, List[str]] = None) -> List[str]:
        """
        Infer likely petroleum products from text and/or matched keywords.
        Returns list of product names in order of likelihood.
        """
        products: Dict[str, int] = {}  # product -> confidence score
        
        # Build search text
        search_text = text.lower()
        if keywords_matched:
            for kw_list in keywords_matched.values():
                search_text += " " + " ".join(kw_list).lower()
        
        # Match against product indicators
        for product, indicators in self.product_map.items():
            for indicator in indicators:
                if indicator.lower() in search_text:
                    products[product] = products.get(product, 0) + 1
        
        # Direct commodity keyword matches are strongest signals
        if keywords_matched:
            commodity_keywords = keywords_matched.get("commodities", [])
            for keyword in commodity_keywords:
                kw_lower = keyword.lower()
                if "bitumen" in kw_lower or "vg-" in kw_lower or "dbm" in kw_lower:
                    products["Bitumen"] = products.get("Bitumen", 0) + 5
                elif "furnace oil" in kw_lower or "fo" in kw_lower or "lshs" in kw_lower:
                    products["Furnace Oil"] = products.get("Furnace Oil", 0) + 5
                    products["LSHS"] = products.get("LSHS", 0) + 3
                elif "hsd" in kw_lower or "diesel" in kw_lower:
                    products["HSD"] = products.get("HSD", 0) + 5
                elif "solvent" in kw_lower or "hexane" in kw_lower or "mto" in kw_lower:
                    products["Solvents"] = products.get("Solvents", 0) + 5
                elif "base oil" in kw_lower:
                    products["Base Oil"] = products.get("Base Oil", 0) + 5
        
        # Sort by confidence and return
        sorted_products = sorted(products.items(), key=lambda x: x[1], reverse=True)
        return [p[0] for p in sorted_products]
    
    def infer_sector(self, text: str, keywords_matched: Dict[str, List[str]] = None) -> str:
        """Infer the industry sector from text and keywords."""
        search_text = text.lower()
        if keywords_matched:
            for kw_list in keywords_matched.values():
                search_text += " " + " ".join(kw_list).lower()
        
        sector_scores: Dict[str, int] = {}
        
        for sector, indicators in self.SECTOR_MAP.items():
            for indicator in indicators:
                if indicator.lower() in search_text:
                    sector_scores[sector] = sector_scores.get(sector, 0) + 1
        
        if sector_scores:
            return max(sector_scores.items(), key=lambda x: x[1])[0]
        return "General Industry"
    
    def enrich_lead(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a lead with inferred product and sector information.
        """
        # Get text content for analysis
        text = ""
        if lead.get("raw_data"):
            raw = lead["raw_data"]
            if isinstance(raw, dict):
                text = " ".join(str(v) for v in raw.values())
            else:
                text = str(raw)
        text += " " + lead.get("company_name", "")
        
        keywords_matched = lead.get("keywords_matched", {})
        
        # Infer products
        products = self.infer_products(text, keywords_matched)
        if products:
            lead["product_match"] = ", ".join(products[:3])  # Top 3
        
        # Infer sector
        if not lead.get("sector"):
            lead["sector"] = self.infer_sector(text, keywords_matched)
        
        return lead
    
    def enrich_batch(self, leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich multiple leads."""
        return [self.enrich_lead(lead) for lead in leads]


# Singleton
_mapper_instance = None

def get_mapper() -> ProductMapper:
    global _mapper_instance
    if _mapper_instance is None:
        _mapper_instance = ProductMapper()
    return _mapper_instance
