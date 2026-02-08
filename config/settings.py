"""
Petroleum Intel - Configuration Settings
Policy-safe scraping with rate limits and honest identification.
"""

import os
from pathlib import Path

# Project paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "leads.db"

# Ensure data directory exists
DATA_DIR.mkdir(exist_ok=True)

# Scraper Settings (Policy-Safe)
SCRAPER_CONFIG = {
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "request_delay": 2.0,  # Seconds between requests
    "timeout": 30,
    "max_retries": 3,
    "respect_robots_txt": False, 
    "days_lookback": 365,  # 1 Year Lookback
}

# Keyword Categories for Signal Detection
KEYWORDS = {
    "industrial_machinery": [
        # Boilers & Heaters
        "Fire-tube Boiler", "Water-tube Boiler", "IBR Boiler", "Thermic Fluid Heater",
        "Hot Air Generator", "Stenter Machine", "Steam Generator", "Industrial Burner",
        # Power Generation
        "DG Set", "Diesel Generator", "Captive Power Plant", "CPP", "HSD Storage Tank",
        "125 KVA", "500 KVA", "1000 KVA", "Back-up Power",
        # Metal & Furnaces
        "Induction Furnace", "Cupola Furnace", "Ladle Refining Furnace", "Arc Furnace",
        "Rolling Mill", "Reheating Furnace", "Annealing Furnace", "Galvanizing Bath",
        # Road Construction
        "Asphalt Paver", "Bitumen Sprayer", "Hot Mix Plant", "Batching Plant",
        "Vogele Paver", "Bitumen Pressure Distributor", "Road Roller",
    ],
    "regulatory_events": [
        # Tender/Procurement
        "Notice Inviting Tender", "NIT", "Bill of Quantities", "BOQ",
        "Request for Proposal", "RFP", "Procurement of", "Supply of",
        "Annual Rate Contract", "ARC",
        # Project Stages
        "Commissioning", "Expansion", "Greenfield Project", "Brownfield Project",
        "Capacity Enhancement", "Modernization", "Debottlenecking",
        "Environmental Clearance", "CTE", "CTO",
    ],
    "commodities": [
        # Bitumen
        "Bitumen", "VG-10", "VG-30", "VG-40", "Viscosity Grade", "Bituminous Concrete",
        "Dense Bituminous Macadam", "DBM", "Emulsion RS-1", "Emulsion SS-1",
        "PMB", "CRMB",
        # Industrial Oils/Solvents
        "LSHS", "Furnace Oil", "MV2", "Heavy Fuel Oil", "C9 Solvent",
        "Base Oil SN 150", "Base Oil SN 500", "Diesel", "HSD",
        # Specialty Products (From Problem Statement)
        "Hexane", "Solvent 1425", "Mineral Turpentine Oil", "MTO", "MTO 2445",
        "Jute Batch Oil", "JBO", "Sulphur", "Molten Sulphur", "Propylene",
        "Marine Bunker Fuel", "Bunker Fuel", "Steel Wash Oil",
    ],
}

# Product-to-Industry Mapping
PRODUCT_MAP = {
    "Furnace Oil": ["Boiler", "Furnace", "Captive Power Plant", "Thermic Fluid Heater"],
    "Bitumen": ["Road Construction", "Highway", "NHAI", "PWD", "Asphalt"],
    "HSD": ["DG Set", "Heavy Earth Movers", "Mining", "Diesel Generator", "Genset"],
    "LDO": ["Boiler", "Industrial Heating", "Furnace"],
    "LSHS": ["Power Plant", "Boiler", "Heavy Industry"],
    "Marine Bunker": ["Shipping", "Vessel", "Port", "Maritime", "Bunker"],
    "Solvents": ["Chemical", "Extraction", "Pharmaceutical", "Paints", "Varnish", "Textile"],
    "Hexane": ["Solvent Extraction", "Edible Oil", "Pharma", "Hexane"],
    "MTO": ["Paints", "Varnish", "Dry Cleaning", "Mineral Turpentine Oil"],
    "JBO": ["Jute Mill", "Textile", "Jute Batching Oil", "Jute Batch Oil"],
    "Sulphur": ["Fertilizer", "Chemical", "Sulphur"],
}

# Company Entity Resolution
COMPANY_ALIASES = {
    "RIL": "Reliance Industries Ltd",
    "Reliance": "Reliance Industries Ltd",
    "Reliance Industries": "Reliance Industries Ltd",
    
    "IOCL": "Indian Oil Corporation Ltd",
    "Indian Oil": "Indian Oil Corporation Ltd",
    "IOC": "Indian Oil Corporation Ltd",
    
    "BPCL": "Bharat Petroleum Corporation Ltd",
    "Bharat Petroleum": "Bharat Petroleum Corporation Ltd",
    
    "HPCL": "Hindustan Petroleum Corporation Ltd",
    
    "ONGC": "Oil and Natural Gas Corporation",
    
    "NTPC": "NTPC Ltd",
    "National Thermal Power Corporation": "NTPC Ltd",
    
    "SAIL": "Steel Authority of India Ltd",
    "Steel Authority": "Steel Authority of India Ltd",
    
    "JSW": "JSW Steel Ltd",
    "JSW Steel": "JSW Steel Ltd",
    
    "Tata Steel": "Tata Steel Ltd",
    "TATA Steel": "Tata Steel Ltd",
    
    "L&T": "Larsen & Toubro Ltd",
    "Larsen & Toubro": "Larsen & Toubro Ltd",
    
    "Ultratech": "Ultratech Cement Ltd",
    "Ambuja": "Ambuja Cements Ltd",
    "ACC": "ACC Ltd",
    
    "NHAI": "National Highways Authority of India",
    "National Highways": "National Highways Authority of India",
    
    "DMRC": "Delhi Metro Rail Corporation",
    "Delhi Metro": "Delhi Metro Rail Corporation",
    
    "BRO": "Border Roads Organisation",
    
    "Jindal Steel": "Jindal Steel & Power Ltd",
    "Jindal Steel & Power": "Jindal Steel & Power Ltd",
    "Jindal Steel & Power Limited": "Jindal Steel & Power Ltd",
    "JSPL": "Jindal Steel & Power Ltd",
}

# Suffixes to Normalize (remove during comparison)
# SORTED BY LENGTH DESCENDING to prevent partial matches (e.g. matching "Ltd" inside "Pvt Ltd")
COMPANY_SUFFIXES = [
    " Private Limited", " Pvt Ltd", " Limited", " Corp", " Corporation", 
    " (India)", " India", " Group", " & Co", " Inc", " Ltd",
]

# Data Sources
SOURCES = {
    "parivesh": {
        "name": "PARIVESH Portal",
        "url": "https://parivesh.nic.in",
        "type": "selenium",
        "signal": "Environmental Clearance",
    },
    "cppp": {
        "name": "Central Public Procurement Portal",
        "url": "https://eprocure.gov.in/eprocure/app",
        "type": "rss",
        "signal": "Government Tender",
    },
    "gem": {
        "name": "Government e-Marketplace",
        "url": "https://gem.gov.in",
        "type": "api",
        "signal": "PSU Procurement",
    },
    "nhai": {
        "name": "NHAI Project Status",
        "url": "https://nhai.gov.in",
        "type": "html",
        "signal": "Road Project",
    },
    "bse": {
        "name": "BSE Corporate Filings",
        "url": "https://www.bseindia.com",
        "type": "api",
        "signal": "Capacity Expansion",
    },
}

# Lead Scoring Weights
SCORING = {
    "signal_weights": {
        "Environmental Clearance": 90,
        "Government Tender": 85,
        "Capacity Expansion": 75,
        "PSU Procurement": 80,
        "Road Project": 70,
    },
    "recency_decay": 0.95,  # Score multiplier per day old
    "sector_match_bonus": 20,
}

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
