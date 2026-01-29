#!/usr/bin/env python3
"""
Quick Quality Fix - Apply Entity Quality Gate and re-qualify existing data
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


def apply_quality_gate():
    """Apply entity quality filtering to targets."""
    print("=" * 60)
    print("🔍 ENTITY QUALITY GATE")
    print("=" * 60)
    
    # Load targets
    targets_path = BASE / "outputs/crm/targets_master_all.csv"
    if not targets_path.exists():
        print("❌ No targets found")
        return None
    
    df = pd.read_csv(targets_path)
    print(f"\nInput: {len(df)} targets")
    
    # Initialize gate
    gate = EntityQualityGate()
    
    # Apply filtering
    leads_list = df.to_dict('records')
    filtered = gate.filter_leads(leads_list)
    
    # Convert back
    filtered_df = pd.DataFrame(filtered)
    
    # Get stats
    stats = gate.get_stats()
    print(f"\n=== FILTERING RESULTS ===")
    print(f"Rejected: {stats['total_rejected']}")
    print(f"Passed: {len(filtered)}")
    print(f"\nGrade Distribution:")
    for grade, count in sorted(stats['grade_distribution'].items()):
        pct = count / len(df) * 100
        print(f"  {grade}: {count} ({pct:.1f}%)")
    
    return filtered_df


def qualify_customers(df):
    """Qualify leads as real stenter customers."""
    print("\n" + "=" * 60)
    print("🎯 CUSTOMER QUALIFICATION")
    print("=" * 60)
    
    # Stenter/OEM keywords for high confidence
    high_confidence_keywords = [
        'brückner', 'bruckner', 'monforts', 'montex',
        'krantz', 'artos', 'santex',
        'stenter', 'spannrahmen', 'ramöz'
    ]
    
    # Medium confidence - textile finishing
    medium_confidence_keywords = [
        'finishing', 'terbiye', 'dyeing', 'boya',
        'heat setting', 'thermofixierung',
        'coating', 'kaplama'
    ]
    
    # Low confidence - textile producer
    low_confidence_keywords = [
        'textile', 'tekstil', 'fabric', 'kumaş',
        'denim', 'cotton', 'polyester',
        'manufacturing', 'üretim', 'mill', 'fabrika'
    ]
    
    def calculate_qualification(row):
        # Combine all text
        text_cols = ['company', 'activities', 'products', 'evidence_snippet', 'description']
        available = [c for c in text_cols if c in row.index and pd.notna(row[c])]
        combined = ' '.join(str(row[c]) for c in available).lower()
        
        # Check keywords
        high_score = sum(1 for kw in high_confidence_keywords if kw in combined)
        medium_score = sum(1 for kw in medium_confidence_keywords if kw in combined)
        low_score = sum(1 for kw in low_confidence_keywords if kw in combined)
        
        if high_score >= 2:
            return 'A', 'Confirmed stenter user - multiple OEM references'
        elif high_score == 1:
            return 'B', 'Likely stenter user - OEM reference found'
        elif medium_score >= 2:
            return 'C', 'Textile finisher - needs verification'
        elif low_score >= 2:
            return 'D', 'Textile producer - may have stenters'
        else:
            return 'E', 'Unqualified - no textile signals'
    
    # Apply qualification
    qualifications = df.apply(calculate_qualification, axis=1)
    df['customer_grade'] = [q[0] for q in qualifications]
    df['qualification_reason'] = [q[1] for q in qualifications]
    
    # Stats
    print(f"\nInput: {len(df)} filtered targets")
    print(f"\n=== QUALIFICATION GRADES ===")
    grade_counts = df['customer_grade'].value_counts().sort_index()
    for grade, count in grade_counts.items():
        pct = count / len(df) * 100
        print(f"  {grade}: {count} ({pct:.1f}%)")
    
    return df


def save_results(df):
    """Save qualified results."""
    print("\n" + "=" * 60)
    print("💾 SAVING RESULTS")
    print("=" * 60)
    
    output_dir = BASE / "outputs/crm"
    
    # All cleaned targets
    all_path = output_dir / "targets_cleaned_all.csv"
    df.to_csv(all_path, index=False)
    print(f"✅ All cleaned: {len(df)} → {all_path.name}")
    
    # Grade A - Confirmed stenter users
    grade_a = df[df['customer_grade'] == 'A']
    grade_a.to_csv(output_dir / "targets_grade_a_confirmed.csv", index=False)
    print(f"✅ Grade A (Confirmed): {len(grade_a)}")
    
    # Grade A+B - Priority targets
    priority = df[df['customer_grade'].isin(['A', 'B'])]
    priority.to_csv(output_dir / "targets_priority_ab.csv", index=False)
    print(f"✅ Priority (A+B): {len(priority)}")
    
    # Grade A+B+C - All qualified
    qualified = df[df['customer_grade'].isin(['A', 'B', 'C'])]
    qualified.to_csv(output_dir / "targets_qualified_abc.csv", index=False)
    print(f"✅ All Qualified (A+B+C): {len(qualified)}")
    
    # Summary by country for priority
    if len(priority) > 0:
        print(f"\n=== PRIORITY TARGETS BY COUNTRY ===")
        print(priority['country'].value_counts().head(10).to_string())
        
        print(f"\n=== PRIORITY TARGETS BY SOURCE ===")
        print(priority['source_type'].value_counts().to_string())
    
    return priority


def main():
    print("\n" + "=" * 70)
    print("🚀 QUICK QUALITY FIX - Clean and Qualify Targets")
    print("=" * 70)
    
    # Step 1: Apply quality gate
    filtered_df = apply_quality_gate()
    if filtered_df is None or len(filtered_df) == 0:
        print("❌ No targets passed quality gate")
        return
    
    # Step 2: Qualify customers
    qualified_df = qualify_customers(filtered_df)
    
    # Step 3: Save results
    priority = save_results(qualified_df)
    
    # Final summary
    print("\n" + "=" * 70)
    print("📋 FINAL SUMMARY")
    print("=" * 70)
    print(f"""
Original targets: loaded from targets_master_all.csv
After quality gate: {len(filtered_df)}
Priority targets (A+B): {len(priority)}

🎯 NEXT STEPS:
1. Review targets_grade_a_confirmed.csv - {len(qualified_df[qualified_df['customer_grade'] == 'A'])} confirmed stenter users
2. Website crawl targets_priority_ab.csv to verify stenter ownership
3. LinkedIn search for decision makers
""")


if __name__ == "__main__":
    main()
