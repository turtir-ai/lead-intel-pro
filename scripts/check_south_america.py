#!/usr/bin/env python3
"""Check South America leads in current data"""
import pandas as pd
import os

os.chdir('/Users/dev/Documents/germany/lead_intel_v2')

# Tüm CSV'leri kontrol et
print('=== LEADS MASTER ===')
df = pd.read_csv('data/processed/leads_master.csv')
print(f'Toplam: {len(df)}')
print()
print('Ülke dağılımı (hepsi):')
print(df['country'].value_counts())
print()

# Güney Amerika var mı?
south_america = ['Brazil', 'Argentina', 'Colombia', 'Chile', 'Peru', 'Ecuador', 'Uruguay', 'Paraguay', 'Bolivia', 'Venezuela']
sa_mask = df['country'].isin(south_america)
print(f'Güney Amerika toplam: {sa_mask.sum()}')
if sa_mask.sum() > 0:
    print(df[sa_mask]['country'].value_counts())
    print()
    print('Güney Amerika şirketleri:')
    print(df[sa_mask][['entity_name', 'country', 'source_type']].to_string())

# Source type dağılımı
print('\n=== SOURCE TYPE ===')
print(df['source_type'].value_counts())

# Raw leads'de neler var?
print('\n=== LEADS RAW ===')
raw = pd.read_csv('data/staging/leads_raw.csv')
print(f'Toplam raw: {len(raw)}')
sa_raw = raw[raw['country'].isin(south_america)]
print(f'Güney Amerika raw: {len(sa_raw)}')
if len(sa_raw) > 0:
    print(sa_raw[['entity_name', 'country', 'source_type']].head(20).to_string())
