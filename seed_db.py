"""
Seed database with sample data for demo purposes.
"""
from datetime import datetime, timedelta
from models.database import get_db
from models.lead import Lead, SignalType, LeadStatus

def seed():
    db = get_db()
    try:
        # 1. High value signal from PARIVESH
        l1 = Lead(
            company_name="UltraGen Power Ltd",
            source="PARIVESH Portal",
            signal_type=SignalType.EC_CLEARANCE,
            source_url="https://parivesh.nic.in/proposal/12345",
            raw_data={"description": "Proposal for 500MW Captive Power Plant expansion in Gujarat using imported coal and furnace oil."},
            discovered_at=datetime.now(),
            score=95.0,
            status=LeadStatus.NEW,
            keywords_matched={"industrial_machinery": ["Captive Power Plant", "Furnace"], "commodities": ["Furnace Oil"]},
            product_match="Furnace Oil",
            sector="Power Generation"
        )
        
        # 2. Tender from CPPP
        l2 = Lead(
            company_name="National Highways Authority of India (NHAI)",
            source="CPPP Tender",
            signal_type=SignalType.TENDER,
            source_url="https://eprocure.gov.in/eprocure/app?tender=56789",
            raw_data={"description": "Notice Inviting Tender for supply of VG-30 Bitumen for NH-44 resurfacing project."},
            discovered_at=datetime.now() - timedelta(hours=4),
            score=88.5,
            status=LeadStatus.NEW,
            keywords_matched={"commodities": ["VG-30", "Bitumen"], "regulatory_events": ["Notice Inviting Tender"]},
            product_match="Bitumen",
            sector="Roads"
        )
        
        # 3. Expansion from BSE
        l3 = Lead(
            company_name="Asian Paints Ltd",
            source="BSE Corporate Filings",
            signal_type=SignalType.EXPANSION,
            source_url="https://bseindia.com/xml-data/corpfiling/Attach/999.pdf",
            raw_data={"description": "Board approves setting up new manufacturing facility at Dahej with 200KL capacity."},
            discovered_at=datetime.now() - timedelta(days=1),
            score=78.0,
            status=LeadStatus.CONTACTED,
            keywords_matched={"regulatory_events": ["Greenfield Project"], "industrial_machinery": ["Boiler"]},
            product_match="Solvents",
            sector="Chemicals"
        )
        
        
        from intelligence.pipeline import IntelligencePipeline
        pipeline = IntelligencePipeline()
        
        leads = [l1, l2, l3]
        processed_leads = [pipeline.process_lead(l.to_dict()) for l in leads]
        
        db.insert_leads_batch(processed_leads)
        print("Seeded 3 valid leads with advanced intelligence.")
    except Exception as e:
        print(f"Error seeding database: {e}")

if __name__ == "__main__":
    seed()
