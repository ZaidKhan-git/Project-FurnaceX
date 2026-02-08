# HPCL Officer Proximity Mapping - Quick Reference

## What This System Does

Automatically assigns the nearest HPCL officer's contact information to every lead in your petroleum intelligence dataset based on geographical proximity.

## Officer Names Now Display As

Since most officers in the HPCL database don't have personal names listed, the system displays them as:

**Format:** `[Role] - [Location]`

**Examples:**
- `Retail Regional Manager - Mumbai`
- `Retail Regional Manager - Lucknow`  
- `Retail Regional Manager - Bhopal`
- `Regional Manager - Kolkata`
- `Retail Regional Manager - Panipat`

## Current Dataset Status

**Total Leads:** 870  
**Successfully Mapped:** 870 (100%)  
**Officers Assigned:** 5 unique officers  
**States Covered:** 5 (West Bengal, Maharashtra, Uttar Pradesh, Madhya Pradesh, Haryana)

### Officer Distribution

| Officer Name | Number of Leads |
|--------------|----------------|
| Retail Regional Manager - Mumbai | 352 |
| Retail Regional Manager - Bhopal | 269 |
| Retail Regional Manager - Lucknow | 118 |
| Regional Manager - Kolkata | 80 |
| Retail Regional Manager - Panipat | 51 |

## How to Use

### Run Officer Mapping on Existing Dataset

```bash
python map_officers_to_leads.py
```

### Run Complete Pipeline (Filtering + Officer Mapping)

```bash
python run_full_pipeline.py
```

### Test with Small Sample

```bash
python test_officer_mapping.py
```

## CSV Columns Added

The system adds 6 new columns to your dataset:

1. **Officer_Name** - Descriptive identifier (Role - Location)
2. **Officer_Phone** - Contact phone number
3. **Officer_Email** - Official email address
4. **Officer_Address** - Office address
5. **Officer_Role** - Official title/position
6. **Officer_Distance_KM** - Distance in kilometers from project

## WhatsApp Integration Ready

All fields needed for automated WhatsApp notifications are now available:

```python
# Example notification data per lead
{
    "officer_name": "Retail Regional Manager - Mumbai",
    "officer_phone": "022-26403955",
    "officer_email": "mum.retrm@hpcl.in",
    "project_name": "Expansion of Data Center",
    "company": "Data Center Holdings India LLP",
    "state": "Maharashtra",
    "priority": "Tier 1 - Immediate",
    "score": 82.2
}
```

## Files Created

- **proximity_service.py** - Core proximity calculation engine
- **map_officers_to_leads.py** - Main mapping script
- **test_officer_mapping.py** - Testing utility
- **run_full_pipeline.py** - Complete automation pipeline

## Configuration

Officer data is stored in:
```
config/hpcl_officers.json
```

To add actual officer names, edit this file and add the name field:
```json
{
    "location": "Mumbai",
    "state": "Maharashtra",
    "role": "Retail Regional Manager",
    "name": "John Doe",  ← Add actual name here
    "email": "mum.retrm@hpcl.in",
    "phone": "022-26403955",
    "address": "..."
}
```

Then re-run: `python map_officers_to_leads.py`

## Next Steps

1. ✅ Officer mapping complete
2. ✅ Data validated and verified
3. 🎯 **Next:** Build WhatsApp notification automation
4. 🎯 **Optional:** Add actual officer names to config file
5. 🎯 **Optional:** Set up automated triggers for new data

---

**System Status:** ✅ Operational and Production-Ready  
**Last Updated:** 2026-02-08  
**Leads Processed:** 870/870 (100%)
