"""
Advanced Lead Scoring Algorithm.
Formula: Total Score = (W1 * Intent) + (W2 * Company Size) + (W3 * Recency)
"""
import logging
from datetime import datetime
from typing import Dict, Any, List

import sys
sys.path.append(str(__file__).rsplit('\\', 2)[0])
from config.settings import SCORING

logger = logging.getLogger(__name__)

class LeadScorer:
    
    # Weights for the scoring formula
    W_INTENT = 0.5
    W_SIZE = 0.2
    W_RECENCY = 0.3
    
    # Intent Scores
    INTENT_SCORES = {
        "Government Tender": 100,
        "PSU Procurement": 90,
        "Procurement Notice": 85,
        "Capacity Expansion": 75,
        "Environmental Clearance": 70,  # High intent but early stage
        "News Article": 50,
        "General": 30
    }
    
    def __init__(self):
        self.recency_decay = SCORING["recency_decay"]
    
    def score_lead(self, lead: Dict[str, Any]) -> float:
        """Calculate weighted lead score."""
        
        # 1. Intent Score
        intent_score = self.INTENT_SCORES.get(lead.get("signal_type"), 30)
        
        # 2. Company Size Score (Inferred from text/keywords)
        size_score = self._infer_size_score(lead)
        
        # 3. Recency Score
        recency_score = self._score_recency(lead.get("discovered_at"))
        
        # Weighted Sum
        total_score = (
            (self.W_INTENT * intent_score) +
            (self.W_SIZE * size_score) +
            (self.W_RECENCY * recency_score)
        )
        
        return min(100, round(total_score, 1))
    
    def _infer_size_score(self, lead: Dict[str, Any]) -> float:
        """Infer company size/opportunity value from text signals."""
        text = str(lead.get("raw_data", "")).lower() + " " + lead.get("description", "").lower()
        
        # High value keywords
        if any(w in text for w in ["mega project", "crores", "500mw", "1000kva", "refinery", "steel plant", "highway", "greenfield"]):
            return 90
        # Medium value
        if any(w in text for w in ["expansion", "modernization", "new unit", "boilers", "capacity"]):
            return 60
        # Default
        return 40

    def _score_recency(self, discovered_at) -> float:
        """Logarithmic decay for recency."""
        if not discovered_at: return 50
        
        if isinstance(discovered_at, str):
            try: discovered_at = datetime.fromisoformat(discovered_at)
            except: return 50
            
        days_old = (datetime.now() - discovered_at).days
        if days_old <= 1: return 100
        if days_old <= 7: return 90
        if days_old <= 30: return 70
        return 40

    def get_priority_level(self, score: float) -> str:
        if score >= 80: return "HOT"
        if score >= 60: return "WARM"
        return "COLD"

# Singleton
_scorer_instance = None
def get_scorer() -> LeadScorer:
    global _scorer_instance
    if _scorer_instance is None:
        _scorer_instance = LeadScorer()
    return _scorer_instance
