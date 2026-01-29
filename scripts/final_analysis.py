#!/usr/bin/env python3
"""Final analysis and export for v4 pipeline."""

import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path('/Users/dev/Documents/germany/lead_intel_v2/outputs/crm')

df = pd.read_csv(OUTPUT_DIR / 'v4_all_leads.csv')

# Quality filter - real companies from reliable sources
quality_mask = (
    (df['entity_grade'] == 'A') & 
    (df['source_type'].isin(['gots', 'oekotex', 'directory', 'fair', 'known_manufacturer']))
)

quality_df = df[quality_mask].copy()

# Export quality leads
quality_df.to_csv(OUTPUT_DIR / 'v4_verified_quality.csv', index=False)
print(f"✅ Exported {len(quality_df)} verified quality leads to v4_verified_quality.csv")

print("\n" + "="*60)
print("PIPELINE V4 FINAL SUMMARY")
print("="*60)

print(f"\n📊 TOPLAM: {len(quality_df)} Doğrulanmış Kaliteli Lead")

print("\n🌍 ÜLKE DAĞILIMI:")
country_counts = quality_df['country'].value_counts().head(10)
for country, count in country_counts.items():
    print(f"  {country}: {count}")

print("\n📁 KAYNAK DAĞILIMI:")
source_counts = quality_df['source_type'].value_counts()
for source, count in source_counts.items():
    print(f"  {source}: {count}")

# Turkish companies
print("\n" + "="*60)
print("🇹🇷 TÜRKİYE - FİNİSHİNG ŞİRKETLERİ (Top 30)")
print("="*60)
turkey = quality_df[quality_df['country'].isin(['Turkey', 'Türkiye'])].sort_values('v4_score', ascending=False)
for i, (_, row) in enumerate(turkey.head(30).iterrows()):
    company = row['company'][:55]
    source = row['source_type']
    print(f"{i+1:2}. {company} | {source}")

# Egyptian companies
print("\n" + "="*60)
print("🇪🇬 MISIR - FİNİSHİNG ŞİRKETLERİ (Top 20)")
print("="*60)
egypt = quality_df[quality_df['country'] == 'Egypt'].sort_values('v4_score', ascending=False)
for i, (_, row) in enumerate(egypt.head(20).iterrows()):
    company = row['company'][:55]
    source = row['source_type']
    print(f"{i+1:2}. {company} | {source}")

# Known Manufacturers (OEM customers)
print("\n" + "="*60)
print("⭐ BİLİNEN ÜRETİCİLER (Manuel Eklenen)")
print("="*60)
known = quality_df[quality_df['source_type'] == 'known_manufacturer']
for i, (_, row) in enumerate(known.iterrows()):
    company = row['company'][:55]
    country = row['country']
    print(f"{i+1:2}. {company} | {country}")

print("\n" + "="*60)
print("ÇIKTI DOSYALARI")
print("="*60)
print(f"  v4_all_leads.csv - {len(df)} leads (tümü)")
print(f"  v4_verified_quality.csv - {len(quality_df)} leads (kaliteli)")
print(f"  v4_customers_only.csv - customer role only")
print(f"  v4_tier1_premium.csv - en yüksek skor")
