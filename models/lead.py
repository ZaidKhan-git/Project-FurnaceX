"""
Lead Model - Data class for petroleum intelligence leads.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Any, Optional


@dataclass
class Lead:
    """Represents a potential petroleum product sales lead."""
    
    company_name: str
    signal_type: str
    source: str
    source_url: Optional[str] = None
    product_match: Optional[str] = None
    sector: Optional[str] = None
    score: float = 0.0
    confidence: float = 0.0
    territory: Optional[str] = None
    sales_officer: Optional[str] = None
    keywords_matched: Dict[str, List[str]] = field(default_factory=dict)
    raw_data: Dict[str, Any] = field(default_factory=dict)
    discovered_at: datetime = field(default_factory=datetime.now)
    status: str = "NEW"
    notes: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        data = asdict(self)
        data["discovered_at"] = self.discovered_at.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Lead":
        """Create Lead from dictionary."""
        if isinstance(data.get("discovered_at"), str):
            data["discovered_at"] = datetime.fromisoformat(data["discovered_at"])
        return cls(**data)
    
    @property
    def is_high_value(self) -> bool:
        """Check if this is a high-value lead (score >= 70)."""
        return self.score >= 70
    
    @property
    def matched_keyword_count(self) -> int:
        """Total number of keywords matched."""
        return sum(len(kws) for kws in self.keywords_matched.values())
    
    def __repr__(self) -> str:
        return (
            f"Lead({self.company_name!r}, "
            f"signal={self.signal_type!r}, "
            f"product={self.product_match!r}, "
            f"score={self.score:.1f})"
        )


# Lead status constants
class LeadStatus:
    NEW = "NEW"
    CONTACTED = "CONTACTED"
    QUALIFIED = "QUALIFIED"
    CONVERTED = "CONVERTED"
    REJECTED = "REJECTED"


# Signal type constants  
class SignalType:
    EC_CLEARANCE = "Environmental Clearance"
    TENDER = "Government Tender"
    EXPANSION = "Capacity Expansion"
    PSU_PROCUREMENT = "PSU Procurement"
    ROAD_PROJECT = "Road Project"
    MEMBER_LIST = "Industry Member"
