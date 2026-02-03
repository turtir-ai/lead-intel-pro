#!/usr/bin/env python3
"""
Deep Website Discovery for SCE Sales-Ready Leads

Uses Brave Search to find real company websites for high-priority leads.
"""

import os
import sys
import time
import requests
from pathlib import Path
from typing import Optional

# Load .env
from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Blocked domains
BLOCKED_DOMAINS = [
    "commonshare.com",
    "opensupplyhub.org",
    "nusalist.com",
    "europages.",
    "mustakbil.com",
    "rehber.corlutso.org.tr",
    "textilegence.com",
    "marketscreener.com",
    "linkedin.com",
    "facebook.com",
    "twitter.com",
    "instagram.com",
    "alibaba.com",
    "made-in-china.com",
    "indiamart.com",
    "tradeindia.com",
    "oeko-tex.com",
    "gots.org",
    "bettercotton.org",
    "dnb.com",
    "bloomberg.com",
    "zoominfo.com",
    "crunchbase.com",
    "owler.com",
    "wikipedia.org",
    "kompass.com",
    "yellowpages",
]


def is_real_website(url: str) -> bool:
    """Check if URL is a real company website."""
    if not url:
        return False
    
    url = url.lower()
    
    for blocked in BLOCKED_DOMAINS:
        if blocked in url:
            return False
    
    return True


def search_brave(query: str, api_key: str) -> list:
    """Search Brave for company website."""
    url = "https://api.search.brave.com/res/v1/web/search"
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key,
    }
    params = {
        "q": query,
        "count": 10,
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        results = []
        for item in data.get("web", {}).get("results", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "description": item.get("description", ""),
            })
        
        return results
    except Exception as e:
        print(f"Brave search error: {e}")
        return []


def find_company_website(company: str, country: str, api_key: str) -> Optional[dict]:
    """Find real company website using Brave search."""
    
    # Build search queries
    queries = [
        f'"{company}" official website',
        f'{company} {country} textile',
        f'{company} site:.com.tr' if 'Türkiye' in country else f'{company} site:.pk' if 'Pakistan' in country else f'{company}',
    ]
    
    for query in queries:
        print(f"  Searching: {query[:50]}...")
        results = search_brave(query, api_key)
        
        for result in results:
            url = result.get("url", "")
            if is_real_website(url):
                # Check if URL contains company name parts
                company_parts = company.lower().split()[:2]  # First 2 words
                url_lower = url.lower()
                
                # Check domain relevance
                if any(part in url_lower for part in company_parts if len(part) > 3):
                    return {
                        "website": url,
                        "title": result.get("title", ""),
                        "description": result.get("description", ""),
                    }
        
        time.sleep(0.5)  # Rate limit
    
    return None


def main():
    api_key = os.getenv("BRAVE_API_KEY")
    if not api_key:
        print("BRAVE_API_KEY not set!")
        return
    
    # High-priority leads that need real websites
    leads = [
        {"company": "ALINDA TEKSTIL BOYA APRE SANAYI VE TICARET ANONIM SIRKETI", "country": "Türkiye"},
        {"company": "Sarena Textile Industries", "country": "Pakistan"},
        {"company": "İstanbul Boyahanesi", "country": "Türkiye"},
        {"company": "Isil Tekstil", "country": "Türkiye"},
        {"company": "Işıl Tekstil San. Ve Tic. Ltd. Sti.", "country": "Türkiye"},
        {"company": "Sapphire Finishing Mills Limited", "country": "Pakistan"},
        {"company": "Allawasaya Textile", "country": "Pakistan"},
        {"company": "Acatel Acabamentos Texteis", "country": "Portekiz"},
        {"company": "Cedro Têxtil", "country": "Brezilya"},
        {"company": "Canatiba", "country": "Brezilya"},
        {"company": "Santista Têxtil", "country": "Brezilya"},
        {"company": "Ottoman Boyahane Apre", "country": "Türkiye"},
        {"company": "Yünsa", "country": "Türkiye"},
        {"company": "Bossa Ticaret", "country": "Türkiye"},
        {"company": "Sanko Tekstil", "country": "Türkiye"},
    ]
    
    print("=" * 60)
    print("DEEP WEBSITE DISCOVERY")
    print("=" * 60)
    print()
    
    found = []
    for lead in leads:
        company = lead["company"]
        country = lead["country"]
        
        print(f"\n{company} ({country}):")
        result = find_company_website(company, country, api_key)
        
        if result:
            print(f"  ✅ Found: {result['website']}")
            found.append({
                "company": company,
                "country": country,
                "website": result["website"],
                "title": result["title"],
            })
        else:
            print(f"  ❌ Not found")
        
        time.sleep(1)  # Rate limit
    
    print("\n" + "=" * 60)
    print(f"RESULTS: Found {len(found)}/{len(leads)} websites")
    print("=" * 60)
    
    for item in found:
        print(f"\n{item['company']}:")
        print(f"  Website: {item['website']}")


if __name__ == "__main__":
    main()
