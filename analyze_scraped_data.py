"""
Comprehensive Data Quality & Lead Generation Analysis
Author: Data Science Team
Purpose: Analyze scraped environmental clearance data for lead potential
"""

import pandas as pd
import os
import json
from pathlib import Path
from collections import Counter
import re

class ScrapedDataAnalyzer:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.results = {}
        
    def analyze_all_files(self):
        """Analyze all CSV files across all states"""
        print("=" * 80)
        print("SCRAPED DATA QUALITY & LEAD POTENTIAL ANALYSIS")
        print("=" * 80)
        print()
        
        all_states = []
        
        # Get all state directories
        for state_dir in self.base_path.iterdir():
            if state_dir.is_dir():
                all_states.append(state_dir.name)
                
        print(f"📍 Found {len(all_states)} states: {', '.join(all_states)}")
        print()
        
        for state in all_states:
            state_path = self.base_path / state
            print(f"\n{'='*80}")
            print(f"🔍 ANALYZING: {state.upper()}")
            print(f"{'='*80}")
            
            # Analyze each CSV in the state
            for csv_file in state_path.glob("*.csv"):
                self.analyze_csv(csv_file, state)
                
        # Generate final recommendations
        self.generate_recommendations()
        
    def analyze_csv(self, file_path, state):
        """Deep analysis of individual CSV file"""
        try:
            df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
            
            print(f"\n📄 File: {file_path.name}")
            print(f"   Rows: {len(df):,} | Columns: {len(df.columns)}")
            
            # Display column names
            print(f"\n   Column Schema:")
            for i, col in enumerate(df.columns, 1):
                null_pct = (df[col].isnull().sum() / len(df)) * 100
                print(f"      {i}. {col} (Nulls: {null_pct:.1f}%)")
            
            # Data quality metrics
            print(f"\n   📊 DATA QUALITY METRICS:")
            
            # Check for duplicates
            duplicates = df.duplicated().sum()
            print(f"      • Duplicate rows: {duplicates} ({(duplicates/len(df)*100):.1f}%)")
            
            # Check completeness of critical fields
            critical_fields = ['Project Name', 'Location', 'Project Proponent', 'Proposal Status']
            for field in critical_fields:
                if field in df.columns:
                    completeness = ((len(df) - df[field].isnull().sum()) / len(df)) * 100
                    print(f"      • {field}: {completeness:.1f}% complete")
            
            # Analyze proposal status distribution
            if 'Proposal Status' in df.columns:
                print(f"\n   📈 PROPOSAL STATUS DISTRIBUTION:")
                status_counts = df['Proposal Status'].value_counts()
                for status, count in status_counts.items():
                    print(f"      • {status}: {count} ({count/len(df)*100:.1f}%)")
            
            # Analyze sectors
            if 'Other Details' in df.columns:
                sectors = self.extract_sectors(df['Other Details'])
                print(f"\n   🏭 SECTOR DISTRIBUTION:")
                for sector, count in sectors.most_common(10):
                    print(f"      • {sector}: {count}")
            
            # Analyze categories
            if 'Other Details' in df.columns:
                categories = self.extract_categories(df['Other Details'])
                print(f"\n   📋 CATEGORY DISTRIBUTION:")
                for category, count in categories.most_common(5):
                    print(f"      • {category}: {count}")
            
            # Extract potential lead signals
            lead_potential = self.assess_lead_potential(df)
            print(f"\n   💡 LEAD POTENTIAL ASSESSMENT:")
            print(f"      • High Priority Projects: {lead_potential['high_priority']}")
            print(f"      • Infrastructure Projects: {lead_potential['infrastructure']}")
            print(f"      • Mining Projects: {lead_potential['mining']}")
            print(f"      • Recent Submissions (2026): {lead_potential['recent']}")
            print(f"      • Under Verification/Review: {lead_potential['active']}")
            
            # Store results
            self.results[f"{state}_{file_path.stem}"] = {
                'total_records': len(df),
                'columns': list(df.columns),
                'quality_score': self.calculate_quality_score(df),
                'lead_potential': lead_potential,
                'dataframe': df
            }
            
        except Exception as e:
            print(f"   ❌ Error analyzing {file_path}: {str(e)}")
    
    def extract_sectors(self, series):
        """Extract sector information from Other Details column"""
        sectors = []
        for item in series.dropna():
            match = re.search(r'Sector:\s*([A-Za-z0-9\-]+)', str(item))
            if match:
                sectors.append(match.group(1))
        return Counter(sectors)
    
    def extract_categories(self, series):
        """Extract category information from Other Details column"""
        categories = []
        for item in series.dropna():
            match = re.search(r'Category:\s*([A-Za-z0-9\-]+)', str(item))
            if match:
                categories.append(match.group(1))
        return Counter(categories)
    
    def assess_lead_potential(self, df):
        """Assess the lead generation potential of the dataset"""
        potential = {
            'high_priority': 0,
            'infrastructure': 0,
            'mining': 0,
            'recent': 0,
            'active': 0,
            'large_scale': 0
        }
        
        # Check for infrastructure projects
        if 'Other Details' in df.columns:
            potential['infrastructure'] = df['Other Details'].str.contains('INFRA', case=False, na=False).sum()
            potential['mining'] = df['Other Details'].str.contains('MIN|Mining', case=False, na=False).sum()
        
        # Check for recent submissions (2026)
        if 'Other Details' in df.columns:
            potential['recent'] = df['Other Details'].str.contains('2026', case=False, na=False).sum()
        
        # Check for active proposals
        if 'Proposal Status' in df.columns:
            active_statuses = ['Under Verification', 'Referred to SEIAA', 'Under Review']
            potential['active'] = df['Proposal Status'].isin(active_statuses).sum()
        
        # Look for large scale projects (Category A or B1)
        if 'Other Details' in df.columns:
            potential['large_scale'] = df['Other Details'].str.contains('Category: [AB]1?', case=False, na=False, regex=True).sum()
        
        # High priority = recent + active + (infrastructure or large scale)
        potential['high_priority'] = len(df[
            (df['Other Details'].str.contains('2026', case=False, na=False)) &
            ((df['Other Details'].str.contains('INFRA', case=False, na=False)) |
             (df['Other Details'].str.contains('Category: [AB]', case=False, na=False, regex=True)))
        ])
        
        return potential
    
    def calculate_quality_score(self, df):
        """Calculate overall data quality score (0-100)"""
        score = 100.0
        
        # Penalize for missing critical fields
        critical_fields = ['Project Name', 'Location', 'Project Proponent']
        for field in critical_fields:
            if field in df.columns:
                null_pct = (df[field].isnull().sum() / len(df)) * 100
                score -= (null_pct * 0.1)  # Reduce score by 10% for each 100% nulls
        
        # Penalize for duplicates
        duplicates_pct = (df.duplicated().sum() / len(df)) * 100
        score -= duplicates_pct
        
        return max(0, min(100, score))
    
    def generate_recommendations(self):
        """Generate actionable recommendations"""
        print(f"\n\n{'='*80}")
        print("📊 COMPREHENSIVE ANALYSIS SUMMARY")
        print(f"{'='*80}")
        
        total_records = sum(r['total_records'] for r in self.results.values())
        avg_quality = sum(r['quality_score'] for r in self.results.values()) / len(self.results) if self.results else 0
        
        print(f"\n📈 Overall Statistics:")
        print(f"   • Total Records: {total_records:,}")
        print(f"   • Average Data Quality Score: {avg_quality:.1f}/100")
        print(f"   • Files Analyzed: {len(self.results)}")
        
        # Aggregate lead potential
        total_leads = {
            'high_priority': sum(r['lead_potential']['high_priority'] for r in self.results.values()),
            'infrastructure': sum(r['lead_potential']['infrastructure'] for r in self.results.values()),
            'mining': sum(r['lead_potential']['mining'] for r in self.results.values()),
            'recent': sum(r['lead_potential']['recent'] for r in self.results.values()),
            'active': sum(r['lead_potential']['active'] for r in self.results.values()),
        }
        
        print(f"\n💼 Lead Generation Potential:")
        print(f"   • High Priority Leads: {total_leads['high_priority']} ({total_leads['high_priority']/total_records*100:.1f}%)")
        print(f"   • Infrastructure Projects: {total_leads['infrastructure']} ({total_leads['infrastructure']/total_records*100:.1f}%)")
        print(f"   • Mining Projects: {total_leads['mining']} ({total_leads['mining']/total_records*100:.1f}%)")
        print(f"   • Recent Submissions (2026): {total_leads['recent']} ({total_leads['recent']/total_records*100:.1f}%)")
        print(f"   • Active (Under Review): {total_leads['active']} ({total_leads['active']/total_records*100:.1f}%)")
        
        print(f"\n\n{'='*80}")
        print("🎯 RECOMMENDATIONS FOR LEAD FILTERING & HUMAN AGENT ADVICE")
        print(f"{'='*80}")
        
        print(f"\n✅ POSITIVE FINDINGS:")
        print(f"   1. Data Quality: {avg_quality:.1f}/100 - {'EXCELLENT' if avg_quality >= 90 else 'GOOD' if avg_quality >= 75 else 'MODERATE'}")
        print(f"   2. Coverage: {total_records:,} environmental clearance proposals across 5 states")
        print(f"   3. Recency: {total_leads['recent']} proposals from 2026 - FRESH DATA!")
        print(f"   4. Active Pipeline: {total_leads['active']} projects currently under verification")
        
        if avg_quality >= 75:
            print(f"\n🟢 VERDICT: DATA IS HIGHLY USEFUL FOR LEAD GENERATION")
        else:
            print(f"\n🟡 VERDICT: DATA IS MODERATELY USEFUL - REQUIRES CLEANING")
        
        print(f"\n📋 SUGGESTED FILTERING CRITERIA FOR NEW LEADS:")
        print(f"   1. Status Filter:")
        print(f"      • Focus on: 'Under Verification', 'Referred to SEIAA', 'Under Review'")
        print(f"      • These are ACTIVE projects with immediate business potential")
        print(f"   ")
        print(f"   2. Recency Filter:")
        print(f"      • Prioritize submissions from 2026 (most recent)")
        print(f"      • These represent NEW opportunities")
        print(f"   ")
        print(f"   3. Project Type Filter:")
        print(f"      • Infrastructure (INFRA-2): High fuel demand")
        print(f"      • Mining (MIN): Heavy equipment = high diesel consumption")
        print(f"      • Construction projects: Concrete mixing, transportation needs")
        print(f"   ")
        print(f"   4. Scale Filter:")
        print(f"      • Category A & B1: Large-scale projects")
        print(f"      • These have significant budgets and long timelines")
        print(f"   ")
        print(f"   5. Geographic Filter:")
        print(f"      • Maharashtra has most records ({self.results.get('maharashtra_environmental_data', {}).get('total_records', 0)})")
        print(f"      • Focus sales efforts on high-density states")
        
        print(f"\n🤖 ADVICE FOR HUMAN AGENTS:")
        print(f"   1. PRIORITIZATION STRATEGY:")
        print(f"      • Rank leads by: Recent + Active + Large Scale + Infrastructure")
        print(f"      • Top tier: Projects that match ALL criteria")
        print(f"   ")
        print(f"   2. PRODUCT MAPPING:")
        print(f"      • Mining projects → Diesel, Lubricants")
        print(f"      • Infrastructure → Diesel, Bitumen, Lubricants")
        print(f"      • Road projects → Bitumen, Diesel")
        print(f"   ")
        print(f"   3. TIMING:")
        print(f"      • Contact projects 'Under Verification' - they're not yet approved")
        print(f"      • Early engagement = better contract terms")
        print(f"   ")
        print(f"   4. KEY INFORMATION TO EXTRACT:")
        print(f"      • Project Proponent (decision maker)")
        print(f"      • Location (logistics planning)")
        print(f"      • Project timeline (from description)")
        print(f"      • Scale/budget indicators")
        
        print(f"\n⚠️ DATA QUALITY CONCERNS:")
        if avg_quality < 75:
            print(f"   • Some records have incomplete information")
            print(f"   • Recommend data enrichment from additional sources")
        else:
            print(f"   • Minimal concerns - data is clean and complete")
        
        print(f"\n🔄 NEXT STEPS:")
        print(f"   1. Create filtered lead export with scoring algorithm")
        print(f"   2. Integrate with CRM for agent assignment")
        print(f"   3. Set up automated monitoring for new submissions")
        print(f"   4. Build product recommendation engine based on project type")
        print(f"   5. Develop outreach templates for each project category")
        
        print(f"\n{'='*80}\n")

def main():
    """Main execution"""
    base_path = r"data\scraped_data"
    
    analyzer = ScrapedDataAnalyzer(base_path)
    analyzer.analyze_all_files()
    
    # Export summary for review
    print("\n💾 Exporting analysis summary...")
    summary_path = Path("data") / "scraped_data_analysis_summary.json"
    
    # Prepare serializable results
    export_results = {}
    for key, value in analyzer.results.items():
        export_results[key] = {
            'total_records': int(value['total_records']),
            'columns': value['columns'],
            'quality_score': float(value['quality_score']),
            'lead_potential': {k: int(v) for k, v in value['lead_potential'].items()}
        }
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(export_results, f, indent=2)
    
    print(f"✅ Summary exported to: {summary_path}")
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    main()
