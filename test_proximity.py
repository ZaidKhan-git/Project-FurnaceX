"""Test proximity scoring with real data"""
import pandas as pd
from geocode_proximity import GeographicProximityScorer

# Read sample lead
df = pd.read_csv('data/filtered_leads_for_agents.csv')
sample = df.iloc[1]  # West Bengal lead

scorer = GeographicProximityScorer()

print(f"Sample Lead:")
print(f"Project: {sample['Project_Name'][:80]}...")
print(f"State: {sample['State']}")
print(f"Location Field: {sample['Location']}")
print(f"Description: {sample['Project_Description'][:150]}...")

# Test with and without project desc
score_without = scorer.calculate_proximity_score(sample['Location'], sample['State'])
score_with = scorer.calculate_proximity_score(sample['Location'], sample['State'], sample['Project_Description'])

info = scorer.get_nearest_depot_info(sample['Location'], sample['State'], sample['Project_Description'])

print(f"\nProximity Score (without desc): {score_without}")
print(f"Proximity Score (with desc): {score_with}")
if info:
    print(f"Nearest Depot: {info['nearest_depot']}")
    print(f"Distance: {info['distance_km']} km")
    print(f"Coordinates: {info['lead_coords']}")
