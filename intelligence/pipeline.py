"""
Central Intelligence Pipeline.
Orchestrates Inference, Routing, and Scoring for all incoming leads.
"""
from typing import Dict, Any
from intelligence.inference import ProductInferenceEngine
from intelligence.routing import GeospatialRouter
from intelligence.scorer import LeadScorer
from intelligence.entity_resolution import CompanyNormalizer
from intelligence.financial_inference import FinancialInferenceEngine

class IntelligencePipeline:
    
    def __init__(self):
        self.inference = ProductInferenceEngine()
        self.router = GeospatialRouter()
        self.scorer = LeadScorer()
        self.normalizer = CompanyNormalizer()
        self.financial_engine = FinancialInferenceEngine()
        
    def process_lead(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a raw lead dictionary with:
        - Entity Resolution (company_name normalization)
        - Product Inference (product_match, confidence)
        - Financial Analysis (capacity %, CAPEX, fuel impact)
        - Geospatial Routing (territory, sales_officer)
        - Advanced Scoring (score)
        """
        # 0. Entity Resolution
        raw_name = lead_data.get("company_name", "")
        lead_data["company_name"] = self.normalizer.normalize(raw_name)
        
        # Combine text sources for analysis
        description = lead_data.get("description", "")
        raw_desc = lead_data.get("raw_data", {}).get("description", "")
        full_text = f"{description} {raw_desc}".strip()
        
        if not full_text:
            return lead_data
            
        # 1. Product Inference
        inf_result = self.inference.analyze_signal(full_text)
        products = inf_result.get("products", [])
        
        if products:
            # Take the highest confidence product
            primary = products[0]
            lead_data["product_match"] = primary["product"]
            lead_data["confidence"] = primary["confidence"]
        else:
            lead_data["confidence"] = 0.0
        
        # 1.5. Financial Inference (for capacity expansion signals)
        signal_type = lead_data.get("signal_type", "")
        sector = lead_data.get("sector", "Manufacturing")
        
        if signal_type in ["Financial Announcement", "Capacity Expansion"]:
            financial_data = self.financial_engine.analyze_financial_signal(full_text, sector)
            
            # Store financial metadata in raw_data
            if "raw_data" not in lead_data:
                lead_data["raw_data"] = {}
            
            lead_data["raw_data"]["capacity_increase_pct"] = financial_data.get("capacity_increase_pct", 0)
            lead_data["raw_data"]["capex_cr"] = financial_data.get("capex_cr", 0)
            lead_data["raw_data"]["fuel_impact"] = financial_data.get("fuel_impact", "Unknown")
            lead_data["raw_data"]["estimated_fuel_increase_pct"] = financial_data.get("estimated_fuel_increase_pct", 0)
            
            # Boost confidence if financial data found
            if financial_data.get("capacity_increase_pct", 0) > 0:
                # Use financial confidence if higher
                lead_data["confidence"] = max(lead_data.get("confidence", 0), financial_data.get("confidence", 0))

        # 1.6. Credit Rating Rationale Analysis
        if signal_type == "Credit Rating Rationale":
            rating_analysis = self.financial_engine.analyze_rating_rationale(full_text)
            
            if "raw_data" not in lead_data: 
                lead_data["raw_data"] = {}
                
            lead_data["raw_data"]["loan_purpose"] = rating_analysis.get("loan_purpose", "General")
            lead_data["raw_data"]["fuel_signals"] = rating_analysis.get("fuel_signals", [])
            
            # If we found specific fuel signals, override product match and boost confidence
            if rating_analysis.get("fuel_signals"):
                primary_signal = rating_analysis["fuel_signals"][0]
                lead_data["product_match"] = primary_signal["product"]
                lead_data["confidence"] = 90.0  # Very high confidence for Rating Rationales

        # 1.7. New Company Registration Analysis
        if signal_type == "New Company Registration":
            # Extract capital from raw_data or description if not present
            capital = lead_data.get("raw_data", {}).get("authorized_capital_cr", 0.0)
            if capital == 0.0:
                # Try to extract from description
                import re
                cap_match = re.search(r'₹(\d+\.?\d*) Cr', lead_data.get("description", ""))
                if cap_match:
                    capital = float(cap_match.group(1))
            
            # Analyze Objects Clause
            objects_analysis = self.financial_engine.analyze_objects_clause(full_text, capital)
            
            if "raw_data" not in lead_data:
                lead_data["raw_data"] = {}
            
            lead_data["raw_data"]["authorized_capital_cr"] = capital
            lead_data["raw_data"]["inferred_needs"] = objects_analysis.get("inferred_needs", [])
            
            if objects_analysis.get("product_match"):
                lead_data["product_match"] = objects_analysis["product_match"]
                lead_data["confidence"] = 75.0  # Good signal but early stage

            
        # 2. Geospatial Routing
        # Check if lat/lon is available in raw_data, otherwise infer
        lat = lead_data.get("raw_data", {}).get("lat")
        lon = lead_data.get("raw_data", {}).get("lon")
        
        route_info = self.router.route_lead(full_text, lat, lon)
        lead_data["territory"] = route_info.get("territory")
        lead_data["sales_officer"] = route_info.get("sales_officer_id")
        
        # 3. Validation & Scoring
        # Ensure discovered_at is present for recency score
        if "discovered_at" not in lead_data:
            from datetime import datetime
            lead_data["discovered_at"] = datetime.now()
            
        score = self.scorer.score_lead(lead_data)
        lead_data["score"] = score
        
        return lead_data
