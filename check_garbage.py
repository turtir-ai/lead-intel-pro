#!/usr/bin/env python3
"""Check for garbage entities in pipeline output"""
import pandas as pd

df = pd.read_csv('/Users/dev/Documents/germany/lead_intel_v2/outputs/crm/targets_master.csv')

# Known stenter machine manufacturers (should NOT be in leads - they are competitors/suppliers)
MACHINE_MANUFACTURERS = [
    'Brückner', 'Bruckner', 'Monforts', 'Santex', 'Ruckh', 
    'Babcock', 'Stenter Machine', 'TexData'
]

# Article title patterns  
ARTICLE_PATTERNS = ['Leader', 'International Textile', 'Finishing Technology']

print('=== GARBAGE KONTROL ===')
print(f'Toplam lead: {len(df)}')
print()

# Check machine manufacturers
print('--- MAKİNE ÜRETİCİLERİ (OLMAMALI) ---')
for kw in MACHINE_MANUFACTURERS:
    matches = df[df['company'].str.contains(kw, case=False, na=False)]
    if len(matches) > 0:
        print(f'{kw}: {len(matches)} eşleşme')
        for _, r in matches.iterrows():
            print(f"  - {r['company'][:60]:<60} | {r['country']}")
        print()

# Check suspicious patterns
print('--- ŞÜPHELİ PATTERN ---')
suspicious = df[df['company'].str.contains(r'Leader|^[A-Z][a-z]+ Textile$|Machines?$', case=False, regex=True, na=False)]
for _, r in suspicious.head(15).iterrows():
    print(f"  - {r['company'][:60]:<60} | {r['country']}")

print()

# South America quality check
print('--- GÜNEY AMERİKA KALİTELİ LİSTESİ ---')
sa = ['Brazil', 'Argentina', 'Peru', 'Colombia', 'Chile', 'Ecuador']
sa_df = df[df['country'].isin(sa)].copy()
print(f'Toplam: {len(sa_df)}')

# Grade distribution
print('\nGrade dağılımı:')
print(sa_df['entity_quality'].value_counts())

# Show all Grade A and B
print('\nGrade A ve B şirketleri:')
good = sa_df[sa_df['entity_quality'].isin(['A', 'B'])]
for _, r in good.iterrows():
    grade = r['entity_quality']
    company = r['company'][:55]
    country = r['country']
    source = r.get('source_type', '?')
    print(f"  [{grade}] {company:<55} | {country:<10} | {source}")
