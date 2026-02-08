"""
Unit tests for Petroleum Intel platform.
"""

import sys
import unittest
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from models.lead import Lead
from intelligence.scorer import LeadScorer
from intelligence.mapper import ProductMapper
from scrapers.base_scraper import BaseScraper


class TestIntelligence(unittest.TestCase):
    
    def setUp(self):
        self.scorer = LeadScorer()
        self.mapper = ProductMapper()
    
    def test_lead_scoring(self):
        """Test lead scoring logic."""
        lead = {
            "signal_type": "Environmental Clearance",  # 90 points
            "discovered_at": datetime.now(),          # Recent (100)
            "keywords_matched": {                     # 3 categories
                "commodities": ["Bitumen"],
                "regulatory_events": ["NIT"],
                "industrial_machinery": ["Boiler"]
            }
        }
        
        # Calculation:
        # Signal: 90 * 0.4 = 36
        # Recency: 100 * 0.3 = 30
        # Keywords: (20+15+10 + 10 bonus) = 55 -> capped 100 * 0.3 = 30
        # Total: 36 + 30 + 30 = 96
        
        score = self.scorer.score_lead(lead)
        self.assertTrue(score > 80, f"Score {score} should be > 80")
        
    def test_product_mapping(self):
        """Test product inference."""
        text = "Construction of new National Highway with bituminous concrete"
        products = self.mapper.infer_products(text)
        
        self.assertIn("Bitumen", products)
        self.assertIn("Roads", self.mapper.infer_sector(text))
        
    def test_mapper_enrichment(self):
        """Test lead enrichment."""
        lead = {
            "company_name": "ABC Road Builders",
            "raw_data": {"desc": "NHAI highway project"},
            "keywords_matched": {"commodities": ["Bitumen"]}
        }
        
        enriched = self.mapper.enrich_lead(lead)
        self.assertEqual(enriched.get("sector"), "Roads")
        self.assertTrue("Bitumen" in enriched.get("product_match"))


class TestBaseScraper(unittest.TestCase):
    
    def test_keyword_matching(self):
        """Test keyword matching logic."""
        # Create a dummy scraper for testing
        class TestScraper(BaseScraper):
            def scrape(self): return []
            def extract_signals(self, data): return []
            
        scraper = TestScraper("test", {})
        
        text = "Supply of VG-30 Bitumen for Road Construction"
        matches = scraper.match_keywords(text)
        
        self.assertIn("VG-30", matches["commodities"])
        self.assertIn("Bitumen", matches["commodities"])
        self.assertIn("Supply of", matches["regulatory_events"])


if __name__ == '__main__':
    unittest.main()
