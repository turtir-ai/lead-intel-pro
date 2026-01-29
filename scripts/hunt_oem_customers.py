#!/usr/bin/env python3
"""
Smart OEM Customer Hunter - Uses multiple sources to find verified stenter customers
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple
import yaml

from src.collectors.discovery.brave_search import BraveSearchClient
from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from bs4 import BeautifulSoup

logger = get_logger(__name__)

# Search queries designed to find REAL stenter customers
OEM_CUSTOMER_QUERIES = [
    # Brückner deliveries
    '"Brückner" "stenter" "delivered" textile',
    '"Brückner" "MONTEX" "installed" customer',
    '"Bruckner" stenter "commissioned" mill',
    'Brückner textile machine order customer 2024',
    'Brückner stenter Egypt Turkey Brazil',
    
    # Monforts deliveries  
    '"Monforts" "stenter" "installed" textile',
    '"Monforts" finishing line delivered customer',
    'Monforts Montex customer reference',
    
    # Textile industry news
    'stenter machine investment textile mill 2024',
    'finishing line installation textile factory',
    '"heat setting" machine textile customer',
    
    # Regional focus
    'stenter installation Egypt textile',
    'stenter Turkey textile factory Brückner',
    'Brazil textile finishing machine',
    'Pakistan textile stenter investment',
]

# Extraction patterns for customer names
CUSTOMER_PATTERNS = [
    # "Company X in Country has installed/ordered"
    (r"([A-Z][A-Za-z0-9\s&.\-']+(?:Ltd|LLC|GmbH|SA|SpA|Inc|Corp|Group|Mills?|Textiles?)?)\s+(?:in|from|of)\s+([A-Z][a-z]+)\s+(?:has\s+)?(?:installed|ordered|commissioned|received|invested)", ['company', 'country']),
    
    # "delivered to Company X"
    (r"delivered\s+to\s+(?:the\s+)?([A-Z][A-Za-z0-9\s&.\-']+(?:Ltd|LLC|GmbH|SA|SpA|Inc|Corp|Group|Mills?|Textiles?)?)", ['company']),
    
    # "at Company X's facility"
    (r"at\s+([A-Z][A-Za-z0-9\s&.\-']+(?:Ltd|LLC|GmbH|SA|SpA|Inc|Corp|Group|Mills?|Textiles?)?)['']?s?\s+(?:facility|plant|factory|mill)", ['company']),
    
    # "Company X has commissioned"
    (r"([A-Z][A-Za-z0-9\s&.\-']+(?:Ltd|LLC|GmbH|SA|SpA|Inc|Corp|Group|Mills?|Textiles?)?)\s+has\s+(?:commissioned|invested|installed|purchased|ordered)", ['company']),
    
    # "Customer: Company X"
    (r"[Cc]ustomer:\s*([A-Z][A-Za-z0-9\s&.\-']+)", ['company']),
]

# OEM brands to exclude (not customers)
OEM_BRANDS = {
    'brückner', 'bruckner', 'brueckner', 'monforts', 'krantz', 
    'santex', 'artos', 'goller', 'babcock', 'strahm', 'benninger'
}

# Countries of interest
TARGET_COUNTRIES = {
    'turkey', 'türkiye', 'egypt', 'morocco', 'tunisia', 'algeria',
    'brazil', 'argentina', 'mexico', 'peru', 'colombia', 'chile',
    'pakistan', 'india', 'bangladesh', 'vietnam', 'indonesia', 'thailand',
    'sri lanka', 'ethiopia', 'kenya', 'south africa'
}


class SmartOEMHunter:
    """Hunt for verified OEM customers using intelligent search."""
    
    def __init__(self, api_key: str):
        self.search = BraveSearchClient(api_key, settings={})
        self.http = HttpClient(settings={}, policies={})
        self.found_customers = []
        
    def hunt(self, queries: List[str], max_per_query: int = 10) -> List[Dict]:
        """
        Execute search queries and extract customer mentions.
        """
        all_urls_seen = set()
        
        for i, query in enumerate(queries, 1):
            print(f"\n[{i}/{len(queries)}] 🔍 {query[:60]}...")
            
            try:
                results = self.search.search(query, count=max_per_query)
            except Exception as e:
                print(f"   ❌ Search error: {e}")
                continue
            
            for res in results:
                url = res.get('url', '')
                if url in all_urls_seen:
                    continue
                all_urls_seen.add(url)
                
                # Skip low-quality domains
                if any(d in url.lower() for d in ['alibaba', 'indiamart', 'youtube', 'facebook', 'linkedin', 'twitter']):
                    continue
                
                # Fetch and parse
                snippet = res.get('description', '')
                title = res.get('title', '')
                
                # Quick snippet analysis first
                customers = self._extract_customers(f"{title} {snippet}", url)
                
                if customers:
                    print(f"   ✓ {url[:60]}... → {len(customers)} customers")
                    self.found_customers.extend(customers)
                else:
                    # Try fetching full page for important domains
                    if any(d in url for d in ['textileworld', 'fibre2fashion', 'textilegence', 'textiles.org']):
                        try:
                            html = self.http.get(url)
                            if html:
                                soup = BeautifulSoup(html, 'html.parser')
                                text = soup.get_text(' ', strip=True)[:10000]
                                customers = self._extract_customers(text, url)
                                if customers:
                                    print(f"   ✓ {url[:60]}... → {len(customers)} customers (deep)")
                                    self.found_customers.extend(customers)
                        except Exception:
                            pass
        
        return self._deduplicate()
    
    def _extract_customers(self, text: str, source_url: str) -> List[Dict]:
        """Extract customer mentions from text."""
        customers = []
        
        for pattern, fields in CUSTOMER_PATTERNS:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                company = match.group(1).strip() if 'company' in fields else ''
                country = match.group(2).strip() if len(fields) > 1 and 'country' in fields else ''
                
                # Clean company name
                company = self._clean_company(company)
                if not company:
                    continue
                
                # Skip OEM brands
                if company.lower() in OEM_BRANDS:
                    continue
                
                # Get context
                start = max(0, match.start() - 100)
                end = min(len(text), match.end() + 100)
                context = text[start:end]
                
                # Detect country from context if not found
                if not country:
                    country = self._detect_country(context)
                
                # Detect OEM brand mentioned
                oem = self._detect_oem(context)
                
                customers.append({
                    'company': company,
                    'country': country or 'Unknown',
                    'oem_brand': oem,
                    'evidence_url': source_url,
                    'evidence_snippet': context[:200],
                    'source_type': 'oem_reference',
                    'confidence': 'high' if oem and country else 'medium',
                    'harvested_at': datetime.now().isoformat()
                })
        
        return customers
    
    def _clean_company(self, name: str) -> str:
        """Clean and validate company name."""
        # Remove common prefixes
        name = re.sub(r'^(the|a|an)\s+', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Too short or too long
        if len(name) < 3 or len(name) > 60:
            return ''
        
        # Must start with capital
        if not name[0].isupper():
            return ''
        
        # Skip generic terms
        generic = {'textile', 'company', 'mill', 'factory', 'group', 'customer', 'client'}
        if name.lower() in generic:
            return ''
        
        return name
    
    def _detect_country(self, text: str) -> str:
        """Detect country from text."""
        text_lower = text.lower()
        for country in TARGET_COUNTRIES:
            if country in text_lower:
                return country.title()
        return ''
    
    def _detect_oem(self, text: str) -> str:
        """Detect OEM brand from text."""
        text_lower = text.lower()
        if 'brückner' in text_lower or 'bruckner' in text_lower:
            return 'Brückner'
        if 'monforts' in text_lower:
            return 'Monforts'
        if 'santex' in text_lower:
            return 'Santex'
        if 'krantz' in text_lower:
            return 'Krantz'
        return ''
    
    def _deduplicate(self) -> List[Dict]:
        """Deduplicate by company name."""
        seen = {}
        for c in self.found_customers:
            key = c['company'].lower()
            if key not in seen or c['confidence'] == 'high':
                seen[key] = c
        return list(seen.values())


def main():
    # Load API key from environment or config
    api_key = os.environ.get('BRAVE_API_KEY') or os.environ.get('Brave_API_KEY')
    
    if not api_key:
        # Try from settings
        settings_path = Path(__file__).parent.parent / "config/settings.yaml"
        with open(settings_path) as f:
            settings = yaml.safe_load(f)
        api_key = settings.get('api_keys', {}).get('brave', '')
    
    if not api_key:
        # Fallback to known key
        api_key = "BSAYTcCa5ZtcjOYZCEduotyNwmZVRXa"
    
    if not api_key:
        print("❌ Brave API key not found")
        return
    
    print("=" * 60)
    print("🎯 SMART OEM CUSTOMER HUNTER")
    print("=" * 60)
    print(f"Running {len(OEM_CUSTOMER_QUERIES)} targeted queries...")
    
    hunter = SmartOEMHunter(api_key)
    customers = hunter.hunt(OEM_CUSTOMER_QUERIES, max_per_query=8)
    
    print()
    print("=" * 60)
    
    if customers:
        df = pd.DataFrame(customers)
        
        # Save to staging
        output_path = Path(__file__).parent.parent / "data/staging/oem_customers.csv"
        df.to_csv(output_path, index=False)
        
        print(f"\n✅ Found {len(df)} verified OEM customers!")
        print(f"   Saved to: {output_path}")
        print()
        
        print("=== TOP 15 CUSTOMERS ===")
        print(df[['company', 'country', 'oem_brand', 'confidence']].head(15).to_string())
        print()
        
        print("=== BY COUNTRY ===")
        print(df['country'].value_counts().head(10).to_string())
        print()
        
        print("=== BY OEM BRAND ===")
        print(df['oem_brand'].value_counts().to_string())
        
        # Merge with existing leads
        raw_path = Path(__file__).parent.parent / "data/staging/leads_raw.csv"
        if raw_path.exists():
            existing = pd.read_csv(raw_path)
            # Add OEM customers
            combined = pd.concat([existing, df], ignore_index=True)
            combined = combined.drop_duplicates(subset=['company'], keep='first')
            combined.to_csv(raw_path, index=False)
            print(f"\n📊 Merged into leads_raw.csv ({len(combined)} total)")
    else:
        print("\n❌ No customers found. Check API key and network.")


if __name__ == "__main__":
    main()
