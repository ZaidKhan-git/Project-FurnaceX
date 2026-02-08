"""
Ministry of Corporate Affairs (MCA) Scraper.
Identifies new company registrations with high authorized capital.
Sources (Simulated): Zauba Corp, Tofler.
"""
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
import random

from scrapers.base_scraper import BaseScraper
from intelligence.financial_inference import FinancialInferenceEngine

logger = logging.getLogger(__name__)

class MCAScraper(BaseScraper):
    """
    Scrapes/Simulates data for New Company Registrations.
    Focus: High Authorized Capital (> 1 Crore) which implies industrial setup.
    """
    
    def __init__(self):
        super().__init__("mca_registry", "https://www.mca.gov.in")
        self.financial_engine = FinancialInferenceEngine()
        
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Fetch new registrations.
        """
        # Simulated data for hackathon
        simulated_registrations = self._get_simulated_registrations()
        return self.extract_signals(simulated_registrations)

    def extract_signals(self, raw_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process raw registrations into intelligence signals."""
        signals = []
        
        for item in raw_items:
            # Filter: Only high capital companies (e.g., > 1 Cr)
            if item["authorized_capital_cr"] < 1.0:
                continue
                
            full_text = f"{item['company_name']} {item['main_objects']}"
            
            # Analyze Objects Clause
            analysis = self.financial_engine.analyze_objects_clause(full_text, item["authorized_capital_cr"])
            
            if analysis["product_match"]:
                signal = {
                    "company_name": item["company_name"],
                    "signal_type": "New Company Registration",
                    "source": "MCA",
                    "source_url": item["url"],
                    "description": f"New Registration: {item['company_name']} with ₹{item['authorized_capital_cr']} Cr Capital. Objects: {item['main_objects'][:200]}...",
                    "raw_data": {
                        "authorized_capital_cr": item["authorized_capital_cr"],
                        "main_objects": item["main_objects"],
                        "registration_date": item["date"],
                        "inferred_needs": analysis["inferred_needs"]
                    },
                    "product_match": analysis["product_match"],
                    "confidence": 75.0, # Good signal but early stage
                    "discovered_at": datetime.now(),
                    "sector": analysis["sector"],
                }
                signals.append(signal)
                
        self.logger.info(f"Extracted {len(signals)} signals from MCA Registrations")
        return signals

    def _get_simulated_registrations(self) -> List[Dict[str, Any]]:
        """Generate realistic new company data."""
        return [
            {
                "company_name": "Solaris Polyesters Pvt Ltd",
                "authorized_capital_cr": 50.0,
                "date": "2026-01-15",
                "url": "https://www.zaubacorp.com/company/SOLARIS-POLYESTERS",
                "main_objects": "To carry on the business of manufacturing, processing, spinning, weaving, and dealing in polyester yarns, fabrics, and synthetic fibers."
            },
            {
                "company_name": "GreenHorizon Agro Foods LLP",
                "authorized_capital_cr": 5.0,
                "date": "2026-01-20",
                "url": "https://www.zaubacorp.com/company/GREENHORIZON",
                "main_objects": "To set up cold storage facilities, food processing units, and deal in agricultural produce."
            },
            {
                "company_name": "Apex Infra Developers Ltd",
                "authorized_capital_cr": 25.0,
                "date": "2026-01-25",
                "url": "https://www.zaubacorp.com/company/APEX-INFRA",
                "main_objects": "To carry on the business of infrastructure development, road construction, highway projects, and real estate development."
            },
            {
                "company_name": "Rapid Tech Solutions Pvt Ltd",
                "authorized_capital_cr": 0.1, # Low capital, should be ignored
                "date": "2026-02-01",
                "url": "https://www.zaubacorp.com/company/RAPID-TECH",
                "main_objects": "To provide software development services and IT consulting."
            }
        ]
