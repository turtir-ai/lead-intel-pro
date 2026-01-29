#!/usr/bin/env python3
"""
OEM Reference Harvester - Pull verified customer names from OEM news pages
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime
from pathlib import Path
from src.collectors.oem_reference_extractor import OEMReferenceExtractor
from src.utils.logger import get_logger

logger = get_logger(__name__)

# OEM Reference Pages - confirmed sources of customer mentions
OEM_SOURCES = {
    'bruckner': [
        'https://www.brueckner-textile.com/en/references/',
        'https://www.brueckner-textile.com/en/news/',
        'https://www.brueckner-textile.com/en/company/references/',
    ],
    'monforts': [
        'https://www.monforts.de/en/references/',
        'https://www.monforts.de/en/news/',
        'https://www.monforts.de/en/company/references/',
    ],
    'santex_rimar': [
        'https://www.santexrimar.com/news/',
        'https://www.santexrimar.com/references/',
    ],
}

# Additional search queries for Google (can use with Brave API)
SEARCH_QUERIES = [
    '"Brückner" "stenter" "installed" site:textileworld.com',
    '"Brückner" "stenter" "delivered" site:textileworld.com',
    '"Monforts" "stenter" "commissioned" site:textileworld.com',
    '"Brückner" "MONTEX" customer',
    '"Monforts" "Montex" "installed at"',
    'Brückner stenter customer reference textile',
    'Monforts finishing line delivered',
]


def main():
    """Extract OEM references and save to CSV."""
    
    print("=" * 60)
    print("🏭 OEM REFERENCE EXTRACTOR")
    print("=" * 60)
    print()
    
    extractor = OEMReferenceExtractor()
    all_mentions = []
    
    # Process each OEM source
    for oem_brand, urls in OEM_SOURCES.items():
        print(f"\n📡 Processing {oem_brand.upper()}...")
        
        for url in urls:
            print(f"   → {url}")
            try:
                mentions = extractor.extract_from_url(url, oem_brand)
                print(f"     Found {len(mentions)} customer mentions")
                all_mentions.extend(mentions)
            except Exception as e:
                print(f"     ❌ Error: {e}")
    
    print()
    print("=" * 60)
    
    if not all_mentions:
        print("❌ No customer mentions found from direct URLs.")
        print("   Try using search-based approach for more results.")
        print()
        
        # Fallback: Use known references from cached HTML
        print("🔍 Scanning cached HTML files for OEM references...")
        html_dir = Path(__file__).parent.parent / "data/raw/html"
        if html_dir.exists():
            count = 0
            for html_file in html_dir.glob("*.html"):
                if count >= 50:  # Limit to 50 files
                    break
                try:
                    content = html_file.read_text(errors='ignore')
                    # Check if OEM-related
                    if any(oem in content.lower() for oem in ['brückner', 'bruckner', 'monforts', 'santex']):
                        mentions = extractor.extract_from_html(content, str(html_file), 'mixed')
                        all_mentions.extend(mentions)
                        count += 1
                except Exception:
                    pass
            print(f"   Found {len(all_mentions)} mentions from cached HTML")
    
    # Convert to DataFrame
    if all_mentions:
        df = pd.DataFrame([
            {
                'company': m.company,
                'country': m.country,
                'oem_brand': m.oem_brand,
                'equipment_type': m.equipment_type,
                'evidence_url': m.evidence_url,
                'evidence_snippet': m.evidence_snippet[:200],
                'confidence': m.confidence,
                'source_type': 'oem_reference',
                'harvested_at': datetime.now().isoformat()
            }
            for m in all_mentions
        ])
        
        # Deduplicate by company
        df = df.drop_duplicates(subset=['company'], keep='first')
        
        # Save
        output_path = Path(__file__).parent.parent / "data/staging/oem_references.csv"
        df.to_csv(output_path, index=False)
        
        print()
        print(f"✅ Saved {len(df)} unique OEM references to:")
        print(f"   {output_path}")
        print()
        print("=== TOP 10 REFERENCES ===")
        print(df[['company', 'country', 'oem_brand', 'confidence']].head(10).to_string())
        
        # Summary by country
        print()
        print("=== ÜLKE DAĞILIMI ===")
        print(df['country'].value_counts().head(10).to_string())
        
    else:
        print("❌ No OEM references found.")
        print()
        print("💡 Öneriler:")
        print("   1. Brave Search API ile OEM haber arama")
        print("   2. TextileWorld.com makalelerini parse etme")
        print("   3. Manuel referans listesi ekleme")


if __name__ == "__main__":
    main()
