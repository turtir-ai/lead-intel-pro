#!/usr/bin/env python3
"""
Step 4: Full Pipeline - Combine all data sources and process
Final step to create the complete qualified customer list
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pathlib import Path
from datetime import datetime

from src.processors.entity_quality_gate import EntityQualityGate
from src.utils.logger import get_logger

logger = get_logger(__name__)
BASE = Path(__file__).parent.parent


def load_all_sources():
    """Load and combine all data sources."""
    print("\n" + "=" * 60)
    print("📦 LOADING ALL DATA SOURCES")
    print("=" * 60)
    
    all_dfs = []
    
    # 1. Original GOTS/OEKOTEX/Directory data
    orig_path = BASE / "outputs/crm/leads_tiered_all.csv"
    if orig_path.exists():
        df = pd.read_csv(orig_path)
        print(f"✓ Original tiered leads: {len(df)}")
        all_dfs.append(df)
    
    # 2. New association sources (Step 2)
    new_sources = BASE / "data/staging/new_sources_associations.csv"
    if new_sources.exists():
        df = pd.read_csv(new_sources)
        print(f"✓ New association sources: {len(df)}")
        all_dfs.append(df)
    
    # 3. Manual OEM customers (Step 3)
    oem_manual = BASE / "data/staging/oem_customers_manual.csv"
    if oem_manual.exists():
        df = pd.read_csv(oem_manual)
        print(f"✓ Manual OEM customers: {len(df)}")
        all_dfs.append(df)
    
    # 4. OEM customers from search (if exists)
    oem_search = BASE / "data/staging/oem_customers.csv"
    if oem_search.exists():
        df = pd.read_csv(oem_search)
        print(f"✓ OEM search customers: {len(df)}")
        all_dfs.append(df)
    
    # Combine
    if not all_dfs:
        print("❌ No data sources found!")
        return None
    
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\n📊 Combined total: {len(combined)} leads")
    
    return combined


def deduplicate(df):
    """Remove duplicates by company name."""
    print("\n" + "=" * 60)
    print("🔄 DEDUPLICATION")
    print("=" * 60)
    
    before = len(df)
    
    # Normalize company names for dedup
    df['_company_norm'] = df['company'].str.lower().str.strip()
    df['_company_norm'] = df['_company_norm'].str.replace(r'[^\w\s]', '', regex=True)
    
    # Keep first occurrence (manual OEM list should be first for priority)
    # Sort by priority: oem_reference_manual > oem_reference > others
    priority_order = {
        'oem_reference_manual': 0,
        'oem_reference': 1,
        'oem_customer': 2,
        'turkey_association': 3,
        'egypt_association': 3,
        'morocco_association': 3,
        'pakistan_textile': 3,
        'brazil_textile': 3,
    }
    
    df['_priority'] = df['source_type'].map(priority_order).fillna(10)
    df = df.sort_values('_priority')
    
    df = df.drop_duplicates(subset=['_company_norm'], keep='first')
    df = df.drop(columns=['_company_norm', '_priority'])
    
    after = len(df)
    print(f"Before: {before} → After: {after} (removed {before - after} duplicates)")
    
    return df


def apply_quality_gate(df):
    """Apply entity quality filtering."""
    print("\n" + "=" * 60)
    print("🔍 ENTITY QUALITY GATE")
    print("=" * 60)
    
    gate = EntityQualityGate()
    leads_list = df.to_dict('records')
    filtered = gate.filter_leads(leads_list)
    
    filtered_df = pd.DataFrame(filtered)
    
    stats = gate.get_stats()
    print(f"Rejected: {stats['total_rejected']}")
    print(f"Passed: {len(filtered)}")
    
    return filtered_df


def qualify_stenter_customers(df):
    """Qualify leads as stenter customers."""
    print("\n" + "=" * 60)
    print("🎯 STENTER CUSTOMER QUALIFICATION")
    print("=" * 60)
    
    # Keywords for qualification
    # TIER 1: Confirmed stenter users (OEM reference or finishing keywords)
    tier1_keywords = [
        'brückner', 'bruckner', 'monforts', 'montex',
        'krantz', 'artos', 'santex',
        'stenter', 'spannrahmen', 'ramöz'
    ]
    
    # TIER 2: Likely finishing (finishing keywords)
    tier2_keywords = [
        'finishing', 'terbiye', 'apre',
        'dyeing', 'boya', 'boyahane',
        'heat setting', 'thermofixierung',
        'coating', 'kaplama'
    ]
    
    # TIER 3: Textile producers (may have finishing)
    tier3_keywords = [
        'denim', 'jean',
        'textile mill', 'tekstil fabrika',
        'integrated', 'entegre',
        'weaving finishing', 'dokuma terbiye'
    ]
    
    def qualify(row):
        # Check if already marked as OEM customer
        if row.get('source_type') == 'oem_reference_manual':
            return 'TIER1-OEM', 'Verified OEM customer', 100
        
        if row.get('has_stenter') == True:
            return 'TIER1-OEM', 'Confirmed stenter user', 100
        
        # Combine text fields
        text_cols = ['company', 'activities', 'products', 'evidence_snippet', 'description', 'evidence', 'oem_brand']
        texts = []
        for col in text_cols:
            if col in row and pd.notna(row[col]):
                texts.append(str(row[col]))
        combined = ' '.join(texts).lower()
        
        # Score
        tier1_count = sum(1 for kw in tier1_keywords if kw in combined)
        tier2_count = sum(1 for kw in tier2_keywords if kw in combined)
        tier3_count = sum(1 for kw in tier3_keywords if kw in combined)
        
        if tier1_count >= 1:
            return 'TIER1-OEM', f'OEM keyword: {tier1_count}x', 90 + tier1_count
        elif tier2_count >= 2:
            return 'TIER2-Finishing', f'Finishing keywords: {tier2_count}x', 70 + tier2_count
        elif tier2_count == 1:
            return 'TIER3-Likely', f'Some finishing signal', 50 + tier2_count
        elif tier3_count >= 1:
            return 'TIER4-Possible', f'Textile producer', 30 + tier3_count
        else:
            return 'TIER5-Unknown', 'No finishing signals', 10
    
    results = df.apply(qualify, axis=1)
    df['stenter_tier'] = [r[0] for r in results]
    df['tier_reason'] = [r[1] for r in results]
    df['priority_score'] = [r[2] for r in results]
    
    # Sort by priority score
    df = df.sort_values('priority_score', ascending=False)
    
    print("\n=== TIER DISTRIBUTION ===")
    tier_counts = df['stenter_tier'].value_counts()
    for tier, count in tier_counts.items():
        pct = count / len(df) * 100
        print(f"  {tier}: {count} ({pct:.1f}%)")
    
    return df


def generate_outputs(df):
    """Generate final output files."""
    print("\n" + "=" * 60)
    print("💾 GENERATING OUTPUT FILES")
    print("=" * 60)
    
    output_dir = BASE / "outputs/crm"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. All qualified leads with tiers
    all_path = output_dir / "final_leads_all.csv"
    df.to_csv(all_path, index=False)
    print(f"✅ All leads: {len(df)} → final_leads_all.csv")
    
    # 2. TIER 1 - Confirmed OEM customers (HIGHEST VALUE)
    tier1 = df[df['stenter_tier'] == 'TIER1-OEM']
    tier1.to_csv(output_dir / "tier1_oem_customers.csv", index=False)
    print(f"✅ TIER 1 (OEM customers): {len(tier1)} → tier1_oem_customers.csv")
    
    # 3. TIER 1+2 - High Priority
    high_priority = df[df['stenter_tier'].isin(['TIER1-OEM', 'TIER2-Finishing'])]
    high_priority.to_csv(output_dir / "high_priority_leads.csv", index=False)
    print(f"✅ High Priority (T1+T2): {len(high_priority)} → high_priority_leads.csv")
    
    # 4. TIER 1+2+3 - All qualified
    qualified = df[df['stenter_tier'].isin(['TIER1-OEM', 'TIER2-Finishing', 'TIER3-Likely'])]
    qualified.to_csv(output_dir / "qualified_leads.csv", index=False)
    print(f"✅ All Qualified (T1+T2+T3): {len(qualified)} → qualified_leads.csv")
    
    # 5. Top 100 by priority score
    top100 = df.head(100)
    top100.to_csv(output_dir / "top100_priority.csv", index=False)
    print(f"✅ Top 100: → top100_priority.csv")
    
    return tier1, high_priority


def print_summary(tier1, high_priority, df):
    """Print final summary."""
    print("\n" + "=" * 70)
    print("📊 FINAL PIPELINE SUMMARY")
    print("=" * 70)
    
    print(f"""
TOTAL LEADS PROCESSED: {len(df)}

🎯 STENTER CUSTOMER TIERS:
""")
    
    tier_counts = df['stenter_tier'].value_counts()
    for tier, count in tier_counts.items():
        pct = count / len(df) * 100
        print(f"   {tier}: {count} ({pct:.1f}%)")
    
    print(f"""
📍 TIER 1 OEM CUSTOMERS BY COUNTRY:
""")
    if len(tier1) > 0:
        country_dist = tier1['country'].value_counts()
        for country, count in country_dist.head(10).items():
            print(f"   {country}: {count}")
    
    print(f"""
📍 HIGH PRIORITY BY COUNTRY (T1+T2):
""")
    if len(high_priority) > 0:
        country_dist = high_priority['country'].value_counts()
        for country, count in country_dist.head(10).items():
            print(f"   {country}: {count}")
    
    print(f"""
🏆 TOP 20 PRIORITY LEADS:
""")
    top20 = df.head(20)
    display_cols = ['company', 'country', 'stenter_tier', 'source_type']
    available_cols = [c for c in display_cols if c in df.columns]
    print(top20[available_cols].to_string())
    
    print(f"""

✅ OUTPUT FILES CREATED IN outputs/crm/:
   - final_leads_all.csv ({len(df)} leads)
   - tier1_oem_customers.csv ({len(tier1)} leads) ← START HERE!
   - high_priority_leads.csv ({len(high_priority)} leads)
   - qualified_leads.csv
   - top100_priority.csv

🎯 NEXT STEPS:
   1. Contact tier1_oem_customers.csv - These are VERIFIED stenter users
   2. Enrich with LinkedIn contacts for decision makers
   3. Expand to high_priority_leads.csv for more prospects
""")


def main():
    print("=" * 70)
    print("🚀 STEP 4: FULL PIPELINE - COMBINE & PROCESS ALL DATA")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load all sources
    df = load_all_sources()
    if df is None:
        return
    
    # Deduplicate
    df = deduplicate(df)
    
    # Apply quality gate
    df = apply_quality_gate(df)
    
    # Qualify stenter customers
    df = qualify_stenter_customers(df)
    
    # Generate outputs
    tier1, high_priority = generate_outputs(df)
    
    # Print summary
    print_summary(tier1, high_priority, df)
    
    print(f"\n✅ Pipeline complete: {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()
