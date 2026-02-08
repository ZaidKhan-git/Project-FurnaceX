"""
Historical Data Seeder for Petroleum Intel.
Generates realistic leads based on actual market patterns for 2025-2026.
"""
import sys
import random
from datetime import datetime, timedelta

sys.path.append(str(__file__).rsplit('\\', 1)[0])
from models.database import get_db
from intelligence.pipeline import IntelligencePipeline

def generate_historical_leads():
    """Generate 100+ realistic leads from Jan 2025 to Feb 2026."""
    
    # Real company names from Indian industrial sector
    companies = [
        "Reliance Industries Ltd", "Tata Steel Ltd", "JSW Steel Ltd",
        "Ultratech Cement Ltd", "ACC Ltd", "Ambuja Cements Ltd",
        "Grasim Industries Ltd", "Century Textiles", "Arvind Ltd",
        "Adani Power Ltd", "NTPC Ltd", "Tata Power Company Ltd",
        "Hindalco Industries", "Vedanta Ltd", "Bharat Petroleum Corp Ltd",
        "Indian Oil Corporation", "Gujarat State Petronet Ltd",
        "National Highways Authority of India", "PWD Maharashtra",
        "Border Roads Organisation", "Delhi Metro Rail Corporation",
        "Larsen & Toubro Ltd", "Shapoorji Pallonji", "Gammon India",
    ]
    
    # Signal templates
    signal_templates = [
        {
            "type": "Capacity Expansion",
            "desc": "Board approves {amount} Crore expansion project for new {facility} at {location}",
            "facilities": ["manufacturing unit", "steel plant", "cement plant", "power plant", "chemical facility", "Paint manufacturing unit", "Jute processing mill"],
            "amounts": ["500", "750", "1200", "2500", "5000"],
            "products": ["Furnace Oil", "HSD", "Bitumen", "LDO", "MTO", "Hexane", "Sulphur"],
        },
        {
            "type": "Government Tender",
            "desc": "NIT for supply of {product} for {purpose} - {quantity}",
            "products": ["VG-30 Bitumen", "HSD (Diesel)", "Furnace Oil", "Lubricants", "Mineral Turpentine Oil", "Jute Batching Oil"],
            "purposes": ["road construction", "power generation", "industrial use", "equipment operation", "paint production", "textile processing"],
            "quantities": ["500 MT", "1000 KL", "2000 MT", "5000 KL"],
        },
        {
            "type": "PSU Procurement",
            "desc": "Procurement of {item} for {project}",
            "items": ["500 KVA DG Sets", "Industrial Boilers", "Captive Power Plant equipment", "HSD Storage Tanks", "Steel Wash Oil"],
            "projects": ["new facility", "maintenance operations", "backup power", "expansion project", "cleaning operations"],
        },
        {
            "type": "Financial Announcement",
            "desc": "Board approves {capex} Crore CAPEX for {capacity_pct}% capacity expansion in {facility}",
            "capex_amounts": ["250", "500", "1000", "1500", "3000"],
            "capacity_pcts": ["15", "20", "25", "30", "40"],
            "facilities": ["Steel manufacturing", "Cement production", "Power generation", "Chemical processing", "Textile operations"],
        },
        {
            "type": "Credit Rating Rationale",
            "desc": "The rating factors in the {rationale_type} for the {project_type} at {location}. {specific_needs}",
            "rationale_types": ["upcoming loan", "term loan facility", "bank facilities rated", "proposed NCDs"],
            "project_types": ["greenfield project", "road widening project", "capacity expansion of the sugar unit", "new textile unit"],
            "specific_needs_map": {
                "greenfield project": "The unit calls for installation of new thermic fluid heaters and boilers.",
                "road widening project": "The project involves 4-laning and bituminous concreting over 120 km.",
                "capacity expansion of the sugar unit": "The setup requires molasses handling and steam generation.",
                "new textile unit": "The project involves installation of 50,000 spindles requiring constant lubrication."
            }
        },
        {
            "type": "New Company Registration",
            "desc": "New Registration: {company} with ₹{capital} Cr Capital. Objects: {objects}",
            "capitals": ["5.0", "10.0", "25.0", "50.0", "100.0"],
            "objects_list": [
                "To carry on the business of manufacturing, processing, spinning, weaving, and dealing in polyester yarns and fabrics.",
                "To set up cold storage facilities, food processing units, and deal in agricultural produce.",
                "To carry on the business of infrastructure development, road construction, highway projects.",
                "To manufacture and deal in bulk drugs, pharmaceutical formulations, and chemical solvents."
            ]
        },
    ]
    
    locations = [
        "Gujarat", "Maharashtra", "Tamil Nadu", "Karnataka", "Delhi", 
        "Uttar Pradesh", "Rajasthan", "West Bengal", "Odisha", "Jharkhand",
        "Haryana", "Punjab", "Madhya Pradesh", "Chhattisgarh", "Andhra Pradesh"
    ]
    
    leads = []
    
    # Generate leads spread across 2025-early 2026
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2026, 2, 8)
    date_range = (end_date - start_date).days
    
    for i in range(120):  # Generate 120 leads
        # Select template
        template = random.choice(signal_templates)
        company = random.choice(companies)
        location = random.choice(locations)
        
        # Generate description
        desc = template["desc"]
        if "{amount}" in desc:
            desc = desc.replace("{amount}", random.choice(template["amounts"]))
        if "{facility}" in desc:
            desc = desc.replace("{facility}", random.choice(template["facilities"]))
        if "{location}" in desc:
            desc = desc.replace("{location}", location)
        if "{product}" in desc:
            desc = desc.replace("{product}", random.choice(template["products"]))
        if "{purpose}" in desc:
            desc = desc.replace("{purpose}", random.choice(template["purposes"]))
        if "{quantity}" in desc:
            desc = desc.replace("{quantity}", random.choice(template["quantities"]))
        if "{item}" in desc:
            desc = desc.replace("{item}", random.choice(template["items"]))
        if "{project}" in desc:
            desc = desc.replace("{project}", random.choice(template["projects"]))
        if "{facility_type}" in desc:
            desc = desc.replace("{facility_type}", random.choice(template["facility_types"]))
        if "{capex}" in desc:
            desc = desc.replace("{capex}", random.choice(template.get("capex_amounts", ["500"])))
        if "{capacity_pct}" in desc:
            desc = desc.replace("{capacity_pct}", random.choice(template.get("capacity_pcts", ["20"])))
        if "{rationale_type}" in desc:
            desc = desc.replace("{rationale_type}", random.choice(template["rationale_types"]))
        if "{project_type}" in desc:
            ptype = random.choice(template["project_types"])
            desc = desc.replace("{project_type}", ptype)
            if "{specific_needs}" in desc:
                needs = template["specific_needs_map"].get(ptype, "")
                desc = desc.replace("{specific_needs}", needs)
        if "{capital}" in desc:
            desc = desc.replace("{capital}", random.choice(template["capitals"]))
        if "{objects}" in desc:
            desc = desc.replace("{objects}", random.choice(template["objects_list"]))
        
        # Random date in range
        days_offset = random.randint(0, date_range)
        discovered_at = start_date + timedelta(days=days_offset)
        
        # Match keywords in description
        from config.settings import KEYWORDS
        keywords_matched = {}
        desc_lower = desc.lower()
        
        for category, keyword_list in KEYWORDS.items():
            matches = []
            for keyword in keyword_list:
                if keyword.lower() in desc_lower:
                    matches.append(keyword)
            if matches:
                keywords_matched[category] = matches
        
        lead = {
            "company_name": company,
            "signal_type": template["type"],
            "source": "BSE" if "Board" in desc else ("GeM" if "Procurement" in template["type"] else "PARIVESH"),
            "source_url": f"https://example.com/signal/{i}",
            "description": desc,
            "keywords_matched": keywords_matched,
            "raw_data": {"description": desc, "location": location},
            "discovered_at": discovered_at,
            "sector": "Industrial",
        }
        
        leads.append(lead)
    
    return leads

def seed_historical_data():
    """Seed database with historical data."""
    print("=" * 80)
    print("SEEDING HISTORICAL DATA (2025-2026)")
    print("=" * 80)
    
    db = get_db()
    pipeline = IntelligencePipeline()
    
    leads = generate_historical_leads()
    print(f"\nGenerating {len(leads)} historical leads...")
    
    # Process through intelligence pipeline
    processed = []
    for lead in leads:
        enriched = pipeline.process_lead(lead)
        processed.append(enriched)
    
    # Filter confidence > 30
    high_quality = [l for l in processed if l.get("confidence", 0) >= 30]
    print(f"High-confidence leads (>30%): {len(high_quality)}")
    
    # Insert to database
    result = db.insert_leads_batch(processed)
    
    print(f"\n✓ Inserted: {result['inserted']} leads")
    print(f"  Duplicates: {result['duplicates']}")
    print(f"  Date range: Jan 2025 - Feb 2026")
    print(f"  Sources: BSE, GeM, PARIVESH (simulated)")
    print("\n" + "=" * 80)

if __name__ == "__main__":
    seed_historical_data()
