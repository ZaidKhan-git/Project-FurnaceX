"""
Financial Inference Engine.
Analyzes corporate filings to estimate fuel consumption impact from capacity changes.
"""
import re
from typing import Dict, Any, Optional, List

class FinancialInferenceEngine:
    """
    Extracts financial signals from corporate announcements:
    - Capacity expansion percentages
    - Investment amounts (CAPEX)
    - Estimated fuel consumption impact
    """
    
    # Regex patterns for capacity extraction
    CAPACITY_PATTERNS = [
        r'(\d+)%\s+(?:capacity\s+)?(?:increase|expansion|enhancement|growth)',  # "20% capacity increase"
        r'(?:increase|expand|enhance)\s+(?:production\s+)?capacity\s+(?:by\s+)?(\d+)%',  # "expand capacity by 20%"
        r'capacity\s+(?:increase\s+)?(?:of\s+)?(\d+)%',  # "capacity of 20%" or "capacity increase of 20%"
        r'(\d+)%\s+more\s+(?:production|output)',  # "20% more production"
        r'(?:increase|enhancement|expansion)\s+of\s+(\d+)%',  # "increase of 30%"
    ]
    
    # CAPEX patterns (in Crores)
    CAPEX_PATTERNS = [
        r'(?:capex|investment|outlay)\s+of\s+(?:rs\.?|inr)?\s*(\d+(?:,\d+)*)\s+crore',  # "CAPEX of Rs 1000 Crore"
        r'(?:rs\.?|inr)?\s*(\d+(?:,\d+)*)\s+crore\s+(?:capex|investment|project)',  # "Rs 1000 Crore investment"
        r'board\s+approves\s+(?:rs\.?|inr)?\s*(\d+(?:,\d+)*)\s+crore',  # "Board approves Rs 1000 Crore"
        r'with\s+(?:rs\.?|inr)?\s*(\d+(?:,\d+)*)\s+(?:cr|crore)',  # "with Rs 1500 Cr"
        r'(?:rs\.?|inr)?\s*(\d+(?:,\d+)*)\s+(?:cr|crore)\s+investment',  # "Rs 1500 Cr investment"
    ]
    
    # Industry-specific fuel consumption baselines
    FUEL_CONSUMPTION_FACTORS = {
        "Steel": {"base_kwh_per_ton": 500, "fuel_intensity": "high"},
        "Cement": {"base_kwh_per_ton": 100, "fuel_intensity": "high"},
        "Chemicals": {"base_kwh_per_ton": 300, "fuel_intensity": "medium"},
        "Textiles": {"base_kwh_per_ton": 50, "fuel_intensity": "low"},
        "Power": {"base_kwh_per_ton": 1000, "fuel_intensity": "extreme"},
        "Metals": {"base_kwh_per_ton": 400, "fuel_intensity": "high"},
    }
    
    def analyze_financial_signal(self, text: str, sector: str = "Manufacturing") -> Dict[str, Any]:
        """
        Analyze corporate announcement for financial indicators.
        
        Returns:
            {
                "capacity_increase_pct": float,
                "capex_cr": float,
                "fuel_impact": str,  # "High", "Medium", "Low"
                "estimated_fuel_increase_pct": float,
                "confidence": float,
            }
        """
        result = {
            "capacity_increase_pct": 0.0,
            "capex_cr": 0.0,
            "fuel_impact": "Unknown",
            "estimated_fuel_increase_pct": 0.0,
            "confidence": 0.0,
        }
        
        # 1. Extract capacity increase percentage
        capacity_pct = self._extract_capacity_percentage(text)
        if capacity_pct:
            result["capacity_increase_pct"] = capacity_pct
            result["confidence"] = 80.0  # High confidence if explicit percentage found
        
        # 2. Extract CAPEX
        capex = self._extract_capex(text)
        if capex:
            result["capex_cr"] = capex
            # Infer capacity from CAPEX if percentage not found
            if not capacity_pct:
                result["capacity_increase_pct"] = self._infer_capacity_from_capex(capex, sector)
                result["confidence"] = 50.0  # Medium confidence for inference
        
        # 3. Estimate fuel consumption impact
        if result["capacity_increase_pct"] > 0:
            # Assumption: Fuel consumption scales linearly with capacity
            result["estimated_fuel_increase_pct"] = result["capacity_increase_pct"]
            
            # Adjust for sector fuel intensity
            sector_factor = self.FUEL_CONSUMPTION_FACTORS.get(sector, {"fuel_intensity": "medium"})
            intensity = sector_factor["fuel_intensity"]
            
            if intensity == "extreme":
                result["fuel_impact"] = "Very High"
                result["estimated_fuel_increase_pct"] *= 1.2  # Power plants use more fuel per unit
            elif intensity == "high":
                result["fuel_impact"] = "High"
            elif intensity == "medium":
                result["fuel_impact"] = "Medium"
            else:
                result["fuel_impact"] = "Low"
        
        return result
    
    def analyze_rating_rationale(self, text: str) -> Dict[str, Any]:
        """
        Analyze 'Rating Rationale' text for specific project signals.
        Maps keywords to fuel/product demand.
        """
        text_lower = text.lower()
        signals = []
        loan_purpose = "General Corporate Purposes"
        
        # 1. Greenfield Projects (New Factory) -> FO + LDO
        if "greenfield" in text_lower or "new plant" in text_lower or "setting up" in text_lower:
            signals.append({
                "signal": "Greenfield Project",
                "product": "Furnace Oil & LDO",
                "reason": "New manufacturing unit requires initial heating/power"
            })
            loan_purpose = "New Project / Greenfield Expansion"
            
        # 2. Road Projects -> Bitumen + HSD
        if "road" in text_lower or "highway" in text_lower or "nhai" in text_lower or "4-laning" in text_lower:
            signals.append({
                "signal": "Road Infrastructure",
                "product": "Bitumen & HSD",
                "reason": "Road construction requires Bitumen and Diesel for machinery"
            })
            loan_purpose = "Infrastructure Development"
            
        # 3. Sugar/Distillery -> Molasses/Ethanol
        if "sugar" in text_lower or "distillery" in text_lower or "ethanol" in text_lower:
            signals.append({
                "signal": "Sugar/Distillery Unit",
                "product": "Bio-Fuels / Sulphur",
                "reason": "Sugar processing and ethanol blending"
            })
            loan_purpose = "Sugar/Ethanol Capacity Expansion"
            
        # 4. Textile/Spinning -> LDO/FO
        if "textile" in text_lower or "spinning" in text_lower or "yarn" in text_lower:
            signals.append({
                "signal": "Textile Unit",
                "product": "Lubricants & Fuel Oil",
                "reason": "Spinning mills require constant lubrication and heating"
            })
            
        return {
            "fuel_signals": signals,
            "loan_purpose": loan_purpose
        }
    
    def analyze_objects_clause(self, text: str, capital_cr: float) -> Dict[str, Any]:
        """
        Infer business needs from 'Main Objects' clause.
        """
        text = text.lower()
        result = {
            "product_match": None,
            "sector": "General",
            "inferred_needs": []
        }
        
        # 1. Polyester/Textile -> Thermic Fluid
        if any(w in text for w in ["polyester", "textile", "spinning", "yarn", "fabric"]):
            result["sector"] = "Textiles"
            result["product_match"] = "Thermic Fluid & Fuel Oil"
            result["inferred_needs"].append("Thermic Fluid Heaters for dyeing/printing")
            
        # 2. Chemical/Solvent -> Fuel Oil / Coal
        elif any(w in text for w in ["chemical", "solvent", "pharmaceutical", "bulk drug"]):
            result["sector"] = "Chemicals"
            result["product_match"] = "Furnace Oil & Coal"
            result["inferred_needs"].append("Steam Boilers for process heat")
            
        # 3. Agro/Food Processing -> HSD/LDO
        elif any(w in text for w in ["food processing", "agro", "dairy", "cold storage"]):
            result["sector"] = "Food Processing"
            result["product_match"] = "HSD & LDO"
            result["inferred_needs"].append("DG Sets and Refrigeration")
            
        # 4. Infrastructure/Real Estate -> Bitumen/HSD
        elif any(w in text for w in ["construction", "infrastructure", "developer", "realty"]):
            result["sector"] = "Infrastructure"
            result["product_match"] = "Bitumen & HSD"
            result["inferred_needs"].append("Construction Machinery")
            
        return result

    def _extract_capacity_percentage(self, text: str) -> Optional[float]:
        """Extract capacity increase percentage from text."""
        text_lower = text.lower()
        
        for pattern in self.CAPACITY_PATTERNS:
            match = re.search(pattern, text_lower, re.IGNORECASE)
            if match:
                try:
                    pct = float(match.group(1))
                    if 0 < pct <= 500:  # Sanity check (max 500% increase)
                        return pct
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _extract_capex(self, text: str) -> Optional[float]:
        """Extract CAPEX amount in Crores."""
        text_lower = text.lower()
        
        for pattern in self.CAPEX_PATTERNS:
            match = re.search(pattern, text_lower, re.IGNORECASE)
            if match:
                try:
                    capex_str = match.group(1).replace(',', '')
                    capex = float(capex_str)
                    if 0 < capex <= 100000:  # Sanity check (max 100k crore)
                        return capex
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _infer_capacity_from_capex(self, capex_cr: float, sector: str) -> float:
        """
        Rough heuristic: Estimate capacity increase from CAPEX.
        Rule of thumb: Every 100 Cr = ~5% capacity for medium-scale industry
        """
        base_estimate = (capex_cr / 100) * 5  # 5% per 100 Cr
        
        # Sector adjustments
        if sector in ["Power", "Steel"]:
            base_estimate *= 0.7  # Capital-intensive, lower % per Cr
        elif sector in ["Textiles", "Chemicals"]:
            base_estimate *= 1.3  # Less capital-intensive
        
        return min(base_estimate, 100)  # Cap at 100%
