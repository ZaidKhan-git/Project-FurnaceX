"""
Product-Need Inference Engine.
Translates vague text signals into concrete product sales opportunities.
Approach: Rule-Based Inference + Contextual Analysis.
"""
import re
from typing import List, Dict, Tuple, Any

from config.settings import PRODUCT_MAP

class ProductInferenceEngine:
    
    # Use centralized configuration
    KEYWORD_MAPPING = PRODUCT_MAP
    
    # Confidence scores
    SCORE_DIRECT = 95
    SCORE_INFERRED = 60
    
    def infer_products(self, text: str) -> List[Dict[str, Any]]:
        """
        Analyze text to find product opportunities with confidence scores.
        Returns list of dicts: {'product': str, 'confidence': int, 'reason': str}
        """
        results = []
        text_lower = text.lower()
        
        for product, keywords in self.KEYWORD_MAPPING.items():
            # Check for direct product mention (e.g. "Furnace Oil")
            if product.lower() in text_lower:
                results.append({
                    "product": product,
                    "confidence": self.SCORE_DIRECT,
                    "reason": "Direct Mention"
                })
                continue
                
            # Check for inferred keywords
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    results.append({
                        "product": product,
                        "confidence": self.SCORE_INFERRED,
                        "reason": f"Inferred from '{keyword}'"
                    })
                    break # Avoid duplicate inferences for same product
                    
        return results

    def analyze_signal(self, text: str) -> Dict[str, Any]:
        """
        Full analysis of a text signal.
        """
        products = self.infer_products(text)
        
        # Contextual analysis (mock NLP for now using regex rules)
        context = "General"
        if re.search(r"(looking for|requirement|urgent|buy|purchase)", text, re.IGNORECASE):
            context = "High Intent (Buying)"
        elif re.search(r"(expansion|new project|setting up|commissioned)", text, re.IGNORECASE):
            context = "Growth Signal (Expansion)"
            
        return {
            "products": products,
            "context": context,
            "primary_product": products[0]['product'] if products else None
        }
