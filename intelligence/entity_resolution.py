"""
Entity Resolution Module.
Normalizes company names to prevent duplicates.
"""
import re
from config.settings import COMPANY_ALIASES, COMPANY_SUFFIXES

class CompanyNormalizer:
    
    def normalize(self, name: str) -> str:
        """
        Normalize company name to canonical form.
        Ex: "RIL" -> "Reliance Industries Ltd"
        Ex: "Jindal Steel & Power Limited" -> "Jindal Steel & Power Ltd"
        """
        if not name:
            return "Unknown"
            
        # 1. Basic Cleanup
        clean_name = name.strip()
        
        # 2. Direct Alias Check (Case-insensitive)
        for alias, canonical in COMPANY_ALIASES.items():
            if clean_name.lower() == alias.lower():
                return canonical
                
        # 3. Suffix Normalization
        # Remove "Limited", "Pvt Ltd" etc to find core name
        core_name = clean_name
        for suffix in COMPANY_SUFFIXES:
            if core_name.lower().endswith(suffix.lower()):
                core_name = core_name[:-len(suffix)].strip()
                break
        
        # 4. Check Alias on Core Name
        # Ex: "RIL Ltd" -> Core "RIL" -> "Reliance Industries Ltd"
        for alias, canonical in COMPANY_ALIASES.items():
            if core_name.lower() == alias.lower():
                return canonical
        
        # 5. If no alias found, return original (or standardized suffix)
        # We prefer "Ltd" over "Limited" if we want strict standards, 
        # but for now, returning the original clean name is safer unless we match an alias.
        # However, to dedupe "Jindal Steel" and "Jindal Steel Ltd", we should probably use the Core Name?
        # No, "Jindal Steel" might be a different company than "Jindal Steel & Power".
        # Safe bet: Return original clean name, rely on Aliases for known equivalents.
        
        return clean_name
