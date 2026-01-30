#!/usr/bin/env python3
"""V5: Add known manufacturers to existing leads"""
import pandas as pd
import sys
import os

os.chdir('/Users/dev/Documents/germany/lead_intel_v2')
sys.path.insert(0, '.')

from src.collectors.known_manufacturers import KnownManufacturersCollector
import yaml

with open('config/targets.yaml') as f:
    targets = yaml.safe_load(f)

# Load existing raw leads
raw = pd.read_csv('data/staging/leads_raw.csv')
print(f'Existing raw leads: {len(raw)}')

# Get known manufacturers
collector = KnownManufacturersCollector(targets_config=targets)
known_leads = collector.harvest()
print(f'Known manufacturers: {len(known_leads)}')

# Convert to DataFrame and append
known_df = pd.DataFrame(known_leads)
combined = pd.concat([raw, known_df], ignore_index=True)

# Dedupe by company name
combined = combined.drop_duplicates(subset=['company'], keep='first')
print(f'Combined after dedupe: {len(combined)}')

# Save
combined.to_csv('data/staging/leads_raw.csv', index=False)
print('Saved to data/staging/leads_raw.csv')

# Show South America count
south_america = ['Brazil', 'Argentina', 'Colombia', 'Chile', 'Peru', 'Ecuador', 'Mexico']
sa = combined[combined['country'].isin(south_america)]
print(f'\nSouth America: {len(sa)}')
print(sa['country'].value_counts())
