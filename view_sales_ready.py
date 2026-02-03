#!/usr/bin/env python3
"""View sales-ready leads from merged output."""

import pandas as pd
import sys
from glob import glob
from pathlib import Path

# Find latest merged file
merged_files = sorted(glob("outputs/crm/verified_merged_*.csv"))
if not merged_files:
    print("No merged files found!")
    sys.exit(1)

latest = merged_files[-1]
print(f"Loading: {latest}")

df = pd.read_csv(latest)

# Clean and export sales-ready
sales_ready = df[df['sce_sales_ready'] == True].copy()

print('=== SCE SALES READY (Kanıtlı Stenter Müşterileri) ===')
print(f'Toplam: {len(sales_ready)}')
print()

for _, row in sales_ready.iterrows():
    print(f"Şirket: {row['company']}")
    print(f"Ülke: {row['country']}")
    print(f"Website: {row['website']}")
    print(f"E-mail: {row['emails']}")
    print(f"Telefon: {row['phones']}")
    print(f"SCE Skoru: {row['sce_total']:.2f}")
    print(f"Sinyaller: {row['sce_signals']}")
    print('-' * 60)
    print()

# Also show leads with contacts but not SCE ready
print("\n=== LEADS WITH CONTACTS (Email/Phone Available) ===")
with_contacts = df[
    (df['sce_sales_ready'] != True) & 
    ((df['emails'].astype(str) != '') & (df['emails'].astype(str) != 'nan') & (df['emails'].astype(str) != '[]'))
].copy()
print(f"Toplam: {len(with_contacts)}")
print()

for _, row in with_contacts.head(20).iterrows():
    print(f"Şirket: {row['company']} ({row['country']})")
    print(f"  Website: {row['website']}")
    print(f"  E-mail: {row['emails']}")
    print()

# Export summary
print("\n=== OVERALL STATS ===")
print(f"Toplam verified lead: {len(df)}")
print(f"Website bulunan: {len(df[df['website'].astype(str) != ''])} ({100*len(df[df['website'].astype(str) != ''])/len(df):.1f}%)")
print(f"Email bulunan: {len(with_contacts) + len(sales_ready)} ({100*(len(with_contacts)+len(sales_ready))/len(df):.1f}%)")
print(f"SCE Sales Ready: {len(sales_ready)} ({100*len(sales_ready)/len(df):.1f}%)")
