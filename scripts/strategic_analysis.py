#!/usr/bin/env python3
"""
Strategic Lead Analysis - Focus on quality over quantity
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pathlib import Path

BASE = Path(__file__).parent.parent


def analyze_finishing_potential():
    """Analyze leads for finishing/stenter potential."""
    
    df = pd.read_csv(BASE / "outputs/crm/targets_cleaned_all.csv")
    
    # Keep only reliable sources
    reliable_sources = ['gots', 'oekotex', 'directory', 'fair', 'oem_customer']
    df = df[df['source_type'].isin(reliable_sources)].copy()
    
    print("=" * 70)
    print("🎯 STRATEGIC LEAD ANALYSIS - Stenter Customer Potential")
    print("=" * 70)
    print(f"\nTotal reliable leads: {len(df)}")
    
    # Business type indicators
    # HIGH value: Companies that DEFINITELY have stenters
    high_value_keywords = [
        'finishing', 'terbiye', 'apre',          # Finishing = needs stenter
        'dyeing', 'boyahane', 'boya tesisi',     # Dyeing often has finishing
        'printing', 'baskı', 'indigo',           # Print houses have stenters
        'denim', 'jean',                          # Denim = finishing intensive
        'coating', 'kaplama', 'laminating',      # Coating uses stenters
        'technical textile', 'teknik tekstil',   # Technical = heat setting
        'nonwoven', 'dokusuz',                   # Nonwoven finishing
    ]
    
    # MEDIUM value: Vertically integrated (may have finishing)
    medium_value_keywords = [
        'integrated', 'entegre',                  # Integrated mills
        'weaving', 'dokuma', 'knitting', 'örme', # May have in-house finishing
        'mill', 'fabrika', 'tesis',              # Manufacturing plants
        'spinning', 'iplik',                      # Spinning + may have finishing
    ]
    
    # Combine text
    text_cols = ['company', 'activities', 'products', 'description']
    available = [c for c in text_cols if c in df.columns]
    df['_text'] = df[available].fillna('').astype(str).apply(' '.join, axis=1).str.lower()
    
    # Score leads
    def score_lead(text):
        high_count = sum(1 for kw in high_value_keywords if kw in text)
        medium_count = sum(1 for kw in medium_value_keywords if kw in text)
        
        if high_count >= 2:
            return 'TIER1-Finishing', 'Multiple finishing indicators'
        elif high_count == 1:
            return 'TIER2-Likely', 'Has finishing capability indicator'
        elif medium_count >= 2:
            return 'TIER3-Possible', 'Integrated producer'
        elif medium_count == 1:
            return 'TIER4-Investigate', 'May have finishing'
        else:
            return 'TIER5-Low', 'No finishing signals'
    
    results = df['_text'].apply(score_lead)
    df['potential_tier'] = [r[0] for r in results]
    df['tier_reason'] = [r[1] for r in results]
    
    print("\n" + "=" * 50)
    print("STENTER CUSTOMER POTENTIAL TIERS")
    print("=" * 50)
    
    tier_counts = df['potential_tier'].value_counts()
    for tier, count in tier_counts.items():
        pct = count / len(df) * 100
        print(f"  {tier}: {count} ({pct:.1f}%)")
    
    # Top tier analysis
    tier1 = df[df['potential_tier'] == 'TIER1-Finishing']
    tier2 = df[df['potential_tier'] == 'TIER2-Likely']
    
    print(f"\n" + "=" * 50)
    print(f"TIER 1 - CONFIRMED FINISHING ({len(tier1)} leads)")
    print("=" * 50)
    
    if len(tier1) > 0:
        print("\nBy Country:")
        print(tier1['country'].value_counts().to_string())
        print("\nSample Companies:")
        display = ['company', 'country', 'source_type']
        print(tier1[display].head(15).to_string())
    
    print(f"\n" + "=" * 50)
    print(f"TIER 2 - LIKELY FINISHING ({len(tier2)} leads)")
    print("=" * 50)
    
    if len(tier2) > 0:
        print("\nBy Country:")
        print(tier2['country'].value_counts().head(8).to_string())
        print("\nSample Companies:")
        display = ['company', 'country', 'source_type']
        print(tier2[display].head(15).to_string())
    
    # Save tiered results
    output_dir = BASE / "outputs/crm"
    
    # Drop temp column
    df.drop(columns=['_text'], inplace=True)
    
    # TIER 1+2 = HIGH PRIORITY
    high_priority = df[df['potential_tier'].isin(['TIER1-Finishing', 'TIER2-Likely'])]
    high_priority.to_csv(output_dir / "leads_high_priority.csv", index=False)
    
    # All tiered
    df.to_csv(output_dir / "leads_tiered_all.csv", index=False)
    
    print(f"\n" + "=" * 50)
    print("FILES SAVED")
    print("=" * 50)
    print(f"✅ High Priority (T1+T2): {len(high_priority)} → leads_high_priority.csv")
    print(f"✅ All Tiered: {len(df)} → leads_tiered_all.csv")
    
    # Strategic recommendations
    print(f"\n" + "=" * 70)
    print("📋 STRATEJİK ÖNERİLER")
    print("=" * 70)
    print(f"""
1. TIER 1 ({len(tier1)} firma) - HEMEN SATIŞA GEÇ
   → Bunlar finishing yapan firmalar, KESİNLİKLE stenter var
   → Website'lerinden contact bilgisi al
   → LinkedIn'de "Maintenance Manager", "Production Manager" ara

2. TIER 2 ({len(tier2)} firma) - ÖNCE DOĞRULA
   → Finishing capability olabilir
   → Website'de "finishing", "terbiye" section var mı kontrol et

3. HEDEF PAZARLAR:
   - Türkiye: {len(df[df['country']=='Turkey'])} firma (en büyük pazar)
   - Mısır: {len(df[df['country']=='Egypt'])} firma (büyüyen pazar)
   - Fas: {len(df[df['country']=='Morocco'])} firma
   
4. EK KAYNAK ÖNERİLERİ:
   - TGSD (Türkiye Giyim Sanayicileri Derneği) üye listesi
   - ETECO (Egypt Textile Export Council) liste
   - Mısır Tekstil Sanayi Odası
   - ITMA 2024 exhibitor listesi (Brückner/Monforts booth visitors)
""")
    
    return high_priority


if __name__ == "__main__":
    result = analyze_finishing_potential()
