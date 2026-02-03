#!/usr/bin/env python3
"""Summary script for final sales export."""

import pandas as pd

# Load final sales list
df = pd.read_csv('outputs/crm/sales_final_20260203_121740.csv')

print('=' * 70)
print('FINAL SALES EXPORT SUMMARY')
print('=' * 70)
print()

# SCE Sales Ready
sce_ready = df[df['sce_sales_ready'] == True]
print(f'🎯 SCE SALES READY (Kanıtlı Stenter Müşterileri): {len(sce_ready)}')
print('-' * 70)
for _, row in sce_ready.iterrows():
    company = str(row['company'])[:50]
    country = row['country']
    website = row['website'] if pd.notna(row['website']) else ''
    score = row['sce_total']
    signals = str(row['sce_signals'])[:50] if pd.notna(row['sce_signals']) else ''
    print(f'  {company}')
    print(f'    Ulke: {country} | SCE: {score:.2f}')
    print(f'    Website: {website}')
    if signals:
        print(f'    Sinyaller: {signals}...')
    print()

# With contacts
with_email = df[df['emails'].astype(str).str.len() > 2]
print(f'📧 EMAIL BULUNAN: {len(with_email)}')
print('-' * 70)
for _, row in with_email.iterrows():
    company = str(row['company'])[:40]
    country = row['country']
    emails = row['emails']
    print(f'  {company} ({country})')
    print(f'    Email: {emails}')
    print()

# Stats by country
print('🌍 ULKE DAGILIMI')
print('-' * 70)
country_counts = df['country'].value_counts()
for country, count in country_counts.items():
    pct = 100 * count / len(df)
    bar = '#' * int(pct / 2)
    print(f'  {country:30} {count:3d} ({pct:5.1f}%) {bar}')

print()
print('=' * 70)
print(f'TOPLAM: {len(df)} verified lead')
website_count = len(df[df['website'].astype(str).str.len() > 5])
print(f'Website bulunan: {website_count} ({100*website_count/len(df):.1f}%)')
print(f'Email bulunan: {len(with_email)} ({100*len(with_email)/len(df):.1f}%)')
print(f'SCE Ready: {len(sce_ready)} ({100*len(sce_ready)/len(df):.1f}%)')
print('=' * 70)
