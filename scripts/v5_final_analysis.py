#!/usr/bin/env python3
"""V5 Final: Export edilen Güney Amerika şirketleri"""
import pandas as pd
import os

os.chdir('/Users/dev/Documents/germany/lead_intel_v2')

df = pd.read_csv('outputs/crm/targets_master.csv')
print('=== EXPORT EDİLEN TARGETS V5 ===')
print(f'Toplam: {len(df)}')

# Güney Amerika
south_america = ['Brazil', 'Argentina', 'Colombia', 'Chile', 'Peru', 'Ecuador', 'Mexico']
sa = df[df['country'].isin(south_america)]
print(f'\nGüney Amerika: {len(sa)}')
print(sa['country'].value_counts())

print('\n=== GÜNEY AMERİKA ŞİRKETLERİ ===')
cols = ['company', 'country', 'source_type']
if 'entity_quality' in df.columns:
    cols.append('entity_quality')
if 'lead_role' in df.columns:
    cols.append('lead_role')
print(sa[cols].head(50).to_string())

# Known manufacturers
print('\n=== KNOWN MANUFACTURERS ===')
known = df[df['source_type'] == 'known_manufacturer']
print(f'Toplam known_manufacturer: {len(known)}')
sa_known = sa[sa['source_type'] == 'known_manufacturer']
print(f'Güney Amerika known_manufacturer: {len(sa_known)}')
if len(sa_known) > 0:
    print(sa_known[['company', 'country']].to_string())
