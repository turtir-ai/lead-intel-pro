#!/usr/bin/env python3
"""V5: Add known manufacturers to enriched leads"""
import pandas as pd
import sys
import os

os.chdir('/Users/dev/Documents/germany/lead_intel_v2')
sys.path.insert(0, '.')

from src.collectors.known_manufacturers import KnownManufacturersCollector
import yaml

with open('config/targets.yaml') as f:
    targets = yaml.safe_load(f)

# Load existing enriched leads
enriched = pd.read_csv('data/staging/leads_enriched.csv')
print(f'Existing enriched leads: {len(enriched)}')

# Get known manufacturers
collector = KnownManufacturersCollector(targets_config=targets)
known_leads = collector.harvest()
print(f'Known manufacturers: {len(known_leads)}')

# Convert to DataFrame
known_df = pd.DataFrame(known_leads)

# Add missing columns that enriched has
for col in enriched.columns:
    if col not in known_df.columns:
        known_df[col] = None

# Append
combined = pd.concat([enriched, known_df], ignore_index=True)

# Check which known manufacturers are NEW
existing_companies = set(enriched['company'].str.lower().str.strip())
new_count = 0
for _, row in known_df.iterrows():
    if row['company'].lower().strip() not in existing_companies:
        new_count += 1
        
print(f'New known manufacturers to add: {new_count}')
print(f'Combined total: {len(combined)}')

# Save
combined.to_csv('data/staging/leads_enriched.csv', index=False)
print('Saved to data/staging/leads_enriched.csv')

# Show South America count
south_america = ['Brazil', 'Argentina', 'Colombia', 'Chile', 'Peru', 'Ecuador', 'Mexico']
sa = combined[combined['country'].isin(south_america)]
print(f'\nSouth America: {len(sa)}')
print(sa['country'].value_counts())
