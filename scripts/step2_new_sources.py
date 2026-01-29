#!/usr/bin/env python3
"""
Step 2: Add New Sources - TGSD, ETECO, Regional Textile Associations
Scrape textile association member lists for finishing companies
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import re
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from bs4 import BeautifulSoup
import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)
BASE = Path(__file__).parent.parent

# Brave API for search
BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY") or os.environ.get("Brave_API_KEY") or "BSAYTcCa5ZtcjOYZCEduotyNwmZVRXa"


class TextileAssociationScraper:
    """Scrape textile association member lists."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.leads = []
        
    def search_brave(self, query: str, count: int = 10) -> List[Dict]:
        """Search using Brave API."""
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {
            "X-Subscription-Token": BRAVE_API_KEY,
            "Accept": "application/json"
        }
        params = {"q": query, "count": count}
        
        try:
            resp = self.session.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return data.get("web", {}).get("results", [])
        except Exception as e:
            print(f"   Brave search error: {e}")
            return []
    
    def extract_company_from_result(self, result: Dict, source: str, country: str) -> Optional[Dict]:
        """Extract company info from search result."""
        title = result.get('title', '')
        url = result.get('url', '')
        snippet = result.get('description', '')
        
        # Skip if it's a directory/list page
        skip_patterns = ['directory', 'list of', 'members', 'üyeleri', 'association']
        if any(p in title.lower() for p in skip_patterns):
            return None
        
        # Extract company name from title
        # Remove common suffixes
        company = title
        for suffix in [' - ', ' | ', ' :: ', ' – ']:
            if suffix in company:
                company = company.split(suffix)[0]
        
        # Clean
        company = company.strip()
        
        if len(company) < 3 or len(company) > 80:
            return None
        
        return {
            'company': company,
            'website': url,
            'country': country,
            'source_type': source,
            'evidence_snippet': snippet[:300],
            'evidence_url': url,
            'harvested_at': datetime.now().isoformat(),
        }
    
    def harvest_turkey_finishing(self):
        """Harvest Turkish textile finishing companies."""
        print("\n🇹🇷 HARVESTING TURKEY - Textile Finishing Companies")
        print("-" * 50)
        
        queries = [
            'türkiye tekstil terbiye firmaları site:.tr',
            'tekstil apre boyama firmaları istanbul bursa',
            'denim finishing turkey textile company',
            'kumaş terbiye tesisi türkiye',
            '"terbiye" "tekstil" şirket site:.tr',
            'TGSD üye listesi tekstil',
            'ITKIB tekstil ihracatçıları finishing',
            'Bursa tekstil terbiye fabrikası',
            'Denizli tekstil boyahane apre',
            'Gaziantep tekstil finishing company',
        ]
        
        for query in queries:
            print(f"   🔍 {query[:50]}...")
            results = self.search_brave(query, count=10)
            
            for res in results:
                company = self.extract_company_from_result(res, 'turkey_association', 'Turkey')
                if company:
                    # Check if finishing related
                    text = (company['company'] + ' ' + company.get('evidence_snippet', '')).lower()
                    if any(kw in text for kw in ['terbiye', 'apre', 'boya', 'finishing', 'denim', 'tekstil']):
                        self.leads.append(company)
                        print(f"      ✓ {company['company'][:40]}")
            
            time.sleep(1)  # Rate limit
    
    def harvest_egypt_textile(self):
        """Harvest Egyptian textile companies."""
        print("\n🇪🇬 HARVESTING EGYPT - Textile Companies")
        print("-" * 50)
        
        queries = [
            'egypt textile finishing company',
            'egyptian textiles dyeing factory',
            'egypt cotton mills finishing',
            'ETECO textile exporters egypt',
            'alexandria textile manufacturing',
            '10th of ramadan textile factory',
            'egyptian textile industry finishing',
            '"egypt" "textile" "finishing" company',
            'cairo textile dyeing company',
        ]
        
        for query in queries:
            print(f"   🔍 {query[:50]}...")
            results = self.search_brave(query, count=10)
            
            for res in results:
                company = self.extract_company_from_result(res, 'egypt_association', 'Egypt')
                if company:
                    text = (company['company'] + ' ' + company.get('evidence_snippet', '')).lower()
                    if any(kw in text for kw in ['textile', 'finishing', 'dyeing', 'cotton', 'fabric', 'mill']):
                        self.leads.append(company)
                        print(f"      ✓ {company['company'][:40]}")
            
            time.sleep(1)
    
    def harvest_morocco_textile(self):
        """Harvest Moroccan textile companies."""
        print("\n🇲🇦 HARVESTING MOROCCO - Textile Companies")
        print("-" * 50)
        
        queries = [
            'morocco textile finishing company',
            'casablanca textile factory dyeing',
            'maroc textile denim finishing',
            'moroccan textile industry',
            'AMITH members textile morocco',
            'tanger textile manufacturing',
        ]
        
        for query in queries:
            print(f"   🔍 {query[:50]}...")
            results = self.search_brave(query, count=8)
            
            for res in results:
                company = self.extract_company_from_result(res, 'morocco_association', 'Morocco')
                if company:
                    text = (company['company'] + ' ' + company.get('evidence_snippet', '')).lower()
                    if any(kw in text for kw in ['textile', 'finishing', 'denim', 'fabric', 'confection']):
                        self.leads.append(company)
                        print(f"      ✓ {company['company'][:40]}")
            
            time.sleep(1)
    
    def harvest_pakistan_textile(self):
        """Harvest Pakistani textile finishing companies."""
        print("\n🇵🇰 HARVESTING PAKISTAN - Textile Finishing")
        print("-" * 50)
        
        queries = [
            'pakistan textile finishing mills',
            'karachi textile dyeing factory',
            'faisalabad textile finishing company',
            'APTMA members pakistan textile',
            'pakistan denim finishing mill',
            'lahore textile processing unit',
        ]
        
        for query in queries:
            print(f"   🔍 {query[:50]}...")
            results = self.search_brave(query, count=8)
            
            for res in results:
                company = self.extract_company_from_result(res, 'pakistan_textile', 'Pakistan')
                if company:
                    text = (company['company'] + ' ' + company.get('evidence_snippet', '')).lower()
                    if any(kw in text for kw in ['textile', 'finishing', 'dyeing', 'mill', 'denim']):
                        self.leads.append(company)
                        print(f"      ✓ {company['company'][:40]}")
            
            time.sleep(1)
    
    def harvest_brazil_textile(self):
        """Harvest Brazilian textile companies."""
        print("\n🇧🇷 HARVESTING BRAZIL - Textile Companies")
        print("-" * 50)
        
        queries = [
            'brazil textile finishing company',
            'ABIT members brazil textile',
            'sao paulo textile dyeing factory',
            'brazil denim finishing mill',
            'acabamento têxtil brasil',
            'tinturaria têxtil brasil',
        ]
        
        for query in queries:
            print(f"   🔍 {query[:50]}...")
            results = self.search_brave(query, count=8)
            
            for res in results:
                company = self.extract_company_from_result(res, 'brazil_textile', 'Brazil')
                if company:
                    text = (company['company'] + ' ' + company.get('evidence_snippet', '')).lower()
                    if any(kw in text for kw in ['textile', 'têxtil', 'finishing', 'acabamento', 'denim', 'tinturaria']):
                        self.leads.append(company)
                        print(f"      ✓ {company['company'][:40]}")
            
            time.sleep(1)
    
    def deduplicate(self):
        """Remove duplicate companies."""
        seen = {}
        unique = []
        
        for lead in self.leads:
            key = lead['company'].lower().strip()
            if key not in seen:
                seen[key] = True
                unique.append(lead)
        
        self.leads = unique
    
    def save_results(self):
        """Save harvested leads."""
        self.deduplicate()
        
        if not self.leads:
            print("\n❌ No leads harvested")
            return None
        
        df = pd.DataFrame(self.leads)
        
        # Save new sources
        output_path = BASE / "data/staging/new_sources_associations.csv"
        df.to_csv(output_path, index=False)
        
        print(f"\n✅ Saved {len(df)} new leads to {output_path.name}")
        
        return df


def main():
    print("=" * 70)
    print("🌍 STEP 2: HARVEST NEW SOURCES - Textile Associations")
    print("=" * 70)
    
    scraper = TextileAssociationScraper()
    
    # Harvest from each region
    scraper.harvest_turkey_finishing()
    scraper.harvest_egypt_textile()
    scraper.harvest_morocco_textile()
    scraper.harvest_pakistan_textile()
    scraper.harvest_brazil_textile()
    
    # Save
    df = scraper.save_results()
    
    if df is not None:
        print("\n" + "=" * 50)
        print("HARVEST SUMMARY")
        print("=" * 50)
        
        print(f"\nTotal new leads: {len(df)}")
        print(f"\nBy Country:")
        print(df['country'].value_counts().to_string())
        print(f"\nBy Source:")
        print(df['source_type'].value_counts().to_string())
        
        print("\n=== SAMPLE NEW LEADS ===")
        print(df[['company', 'country', 'website']].head(20).to_string())
    
    return df


if __name__ == "__main__":
    main()
