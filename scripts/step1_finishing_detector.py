#!/usr/bin/env python3
"""
Finishing Detector - Crawl websites to identify finishing capabilities
Step 1 of full pipeline enhancement
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import re
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from bs4 import BeautifulSoup

logger = get_logger(__name__)
BASE = Path(__file__).parent.parent

# Keywords that indicate FINISHING capability (= uses stenters)
FINISHING_KEYWORDS = {
    # English
    'finishing': 10,
    'stenter': 15,
    'stentering': 15,
    'heat setting': 12,
    'heat-setting': 12,
    'thermosetting': 12,
    'sanforizing': 10,
    'mercerizing': 8,
    'calendering': 8,
    'coating': 8,
    'laminating': 8,
    'dyeing': 6,
    'printing': 5,
    'bleaching': 5,
    
    # Turkish
    'terbiye': 12,
    'apre': 10,
    'ram makinesi': 15,
    'ramöz': 15,
    'spannrahmen': 15,
    'boyahane': 8,
    'boya tesisi': 8,
    'baskı': 5,
    'kaplama': 8,
    
    # German
    'veredelung': 10,
    'ausrüstung': 10,
    'färberei': 8,
    
    # Arabic transliteration
    'tashteeb': 10,
    
    # Equipment brands
    'brückner': 15,
    'bruckner': 15,
    'monforts': 15,
    'montex': 15,
    'krantz': 12,
    'artos': 12,
    'santex': 12,
}

# Business indicators
PRODUCTION_KEYWORDS = {
    'manufacturing': 3,
    'production': 3,
    'factory': 3,
    'mill': 3,
    'plant': 3,
    'facility': 3,
    'capacity': 3,
    'üretim': 3,
    'fabrika': 3,
    'tesis': 3,
}


class FinishingDetector:
    """Detect finishing capabilities by crawling company websites."""
    
    def __init__(self):
        self.http = HttpClient(settings={'timeout': 15}, policies={})
        self.results = []
        self.crawled = 0
        self.errors = 0
        
    def detect_finishing(self, url: str, company: str) -> Dict:
        """
        Crawl a website and detect finishing indicators.
        
        Returns:
            Dict with finishing_score, keywords_found, has_finishing
        """
        result = {
            'company': company,
            'website': url,
            'finishing_score': 0,
            'keywords_found': [],
            'has_finishing': False,
            'confidence': 'none',
            'crawl_status': 'unknown',
        }
        
        if not url or pd.isna(url) or url == 'nan':
            result['crawl_status'] = 'no_website'
            return result
        
        # Normalize URL
        if not url.startswith('http'):
            url = 'https://' + url
        
        try:
            html = self.http.get(url)
            if not html:
                result['crawl_status'] = 'fetch_failed'
                return result
            
            result['crawl_status'] = 'success'
            
            # Parse and extract text
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove unwanted elements
            for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
                tag.decompose()
            
            text = soup.get_text(' ', strip=True).lower()
            
            # Also check meta tags
            meta_content = ''
            for meta in soup.find_all('meta'):
                content = meta.get('content', '')
                if content:
                    meta_content += ' ' + content.lower()
            
            full_text = text + ' ' + meta_content
            
            # Score based on keywords
            score = 0
            found = []
            
            for keyword, points in FINISHING_KEYWORDS.items():
                if keyword in full_text:
                    score += points
                    found.append(keyword)
            
            # Add production bonus
            for keyword, points in PRODUCTION_KEYWORDS.items():
                if keyword in full_text:
                    score += points
            
            result['finishing_score'] = score
            result['keywords_found'] = found
            
            # Determine confidence
            if score >= 30:
                result['has_finishing'] = True
                result['confidence'] = 'high'
            elif score >= 15:
                result['has_finishing'] = True
                result['confidence'] = 'medium'
            elif score >= 8:
                result['has_finishing'] = False
                result['confidence'] = 'low'
            else:
                result['has_finishing'] = False
                result['confidence'] = 'none'
            
            self.crawled += 1
            
        except Exception as e:
            result['crawl_status'] = f'error: {str(e)[:50]}'
            self.errors += 1
        
        return result
    
    def process_batch(self, leads: List[Dict], max_workers: int = 5) -> List[Dict]:
        """Process a batch of leads with parallel crawling."""
        results = []
        total = len(leads)
        
        print(f"\n🌐 Crawling {total} websites...")
        print(f"   Using {max_workers} parallel workers")
        print()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            
            for lead in leads:
                url = lead.get('website', '')
                company = lead.get('company', '')
                
                future = executor.submit(self.detect_finishing, url, company)
                futures[future] = lead
            
            for i, future in enumerate(as_completed(futures), 1):
                lead = futures[future]
                try:
                    result = future.result()
                    # Merge with original lead
                    lead.update(result)
                    results.append(lead)
                    
                    # Progress
                    if i % 25 == 0 or i == total:
                        pct = i / total * 100
                        print(f"   [{i}/{total}] {pct:.1f}% - Crawled: {self.crawled}, Errors: {self.errors}")
                    
                except Exception as e:
                    lead['crawl_status'] = f'thread_error: {e}'
                    results.append(lead)
        
        return results


def main():
    """Run finishing detection on reliable leads."""
    
    print("=" * 70)
    print("🔍 STEP 1: FINISHING DETECTION VIA WEBSITE CRAWL")
    print("=" * 70)
    
    # Load reliable leads
    input_path = BASE / "outputs/crm/leads_tiered_all.csv"
    if not input_path.exists():
        input_path = BASE / "outputs/crm/targets_cleaned_all.csv"
    
    if not input_path.exists():
        print("❌ No input data found")
        return
    
    df = pd.read_csv(input_path)
    print(f"\nLoaded {len(df)} leads from {input_path.name}")
    
    # Filter to those with websites
    has_website = df[df['website'].notna() & (df['website'] != '') & (df['website'] != 'nan')]
    print(f"Leads with websites: {len(has_website)}")
    
    # Initialize detector
    detector = FinishingDetector()
    
    # Process
    leads_list = has_website.to_dict('records')
    
    # Limit for testing - remove this for full run
    # leads_list = leads_list[:50]
    
    results = detector.process_batch(leads_list, max_workers=5)
    
    # Convert to DataFrame
    results_df = pd.DataFrame(results)
    
    # Stats
    print("\n" + "=" * 50)
    print("CRAWL RESULTS")
    print("=" * 50)
    
    status_counts = results_df['crawl_status'].value_counts()
    print("\nCrawl Status:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")
    
    finishing = results_df[results_df['has_finishing'] == True]
    print(f"\n✅ Companies with FINISHING capability: {len(finishing)}")
    
    if len(finishing) > 0:
        print("\nBy Confidence:")
        print(finishing['confidence'].value_counts().to_string())
        
        print("\nBy Country:")
        print(finishing['country'].value_counts().head(10).to_string())
        
        print("\n=== TOP FINISHING COMPANIES ===")
        top = finishing.nlargest(20, 'finishing_score')
        display_cols = ['company', 'country', 'finishing_score', 'keywords_found']
        print(top[display_cols].to_string())
    
    # Save results
    output_dir = BASE / "outputs/crm"
    
    # All results with crawl data
    results_df.to_csv(output_dir / "leads_crawled.csv", index=False)
    
    # Only finishing companies
    if len(finishing) > 0:
        finishing.to_csv(output_dir / "leads_finishing_confirmed.csv", index=False)
    
    print(f"\n✅ Saved crawl results to outputs/crm/")
    
    return results_df


if __name__ == "__main__":
    main()
