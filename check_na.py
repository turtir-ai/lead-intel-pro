import pandas as pd

# Check targets_master.csv
df = pd.read_csv('/Users/dev/Documents/germany/lead_intel_v2/outputs/crm/targets_master.csv')
print('=== TARGETS_MASTER.CSV ===')
print(f'Total: {len(df)}')
print()
print('All countries:')
print(df['country'].value_counts().to_string())
print()

# Check for North Africa countries
na_countries = ['Egypt', 'Morocco', 'Tunisia', 'Algeria', 'Libya']
na_df = df[df['country'].isin(na_countries)]
print(f'North Africa in targets_master: {len(na_df)}')

# Check leads_master.csv
master = pd.read_csv('/Users/dev/Documents/germany/lead_intel_v2/data/processed/leads_master.csv')
print()
print('=== LEADS_MASTER.CSV ===')
na_master = master[master['country'].isin(na_countries)]
print(f'North Africa in leads_master: {len(na_master)}')
if len(na_master) > 0:
    print(na_master['country'].value_counts().to_string())
    print()
    print('Has website:', na_master['website'].notna().sum())
    print('Website empty string:', (na_master['website'] == '').sum())
    if 'source_type' in na_master.columns:
        print('Source types:')
        print(na_master['source_type'].value_counts().to_string())
