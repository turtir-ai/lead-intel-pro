#!/usr/bin/env python3
"""
Trade Fair Exhibitor Collector

Automatically finds and scrapes textile trade fair exhibitor lists:
1. Heimtextil (Frankfurt)
2. Techtextil (Frankfurt)  
3. ITMA (global)
4. Texworld (Paris)
5. Cairo Fashion & Tex
6. Premiere Vision
7. Istanbul Textile Fair

Uses Brave API to find current exhibitor lists.
"""

import re
import os
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import requests
import trafilatura

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


class FairExhibitorCollector:
    """
    Collects exhibitor lists from textile trade fairs.
    Trade fair exhibitors are HIGH VALUE leads - they're active buyers!
    """
    
    # Major textile fairs
    FAIRS = [
        {
            "name": "Heimtextil",
            "search_queries": [
                "Heimtextil exhibitor list 2025",
                "Heimtextil aussteller liste",
                "site:heimtextil.messefrankfurt.com exhibitor",
            ],
            "base_url": "https://heimtextil.messefrankfurt.com/",
        },
        {
            "name": "Techtextil",
            "search_queries": [
                "Techtextil exhibitor list 2025",
                "Techtextil aussteller",
                "site:techtextil.messefrankfurt.com exhibitor",
            ],
            "base_url": "https://techtextil.messefrankfurt.com/",
        },
        {
            "name": "ITMA",
            "search_queries": [
                "ITMA textile exhibitor list",
                "ITMA Asia exhibitor directory",
                "site:itma.com exhibitors",
            ],
            "base_url": "https://www.itma.com/",
        },
        {
            "name": "Cairo Fashion Tex",
            "search_queries": [
                "Cairo Fashion Tex exhibitor list",
                "Cairo textile fair exhibitors",
                "Egypt textile exhibition companies",
            ],
            "base_url": "https://cairofashiontex.com/",
        },
        {
            "name": "Istanbul Textile",
            "search_queries": [
                "ITM Istanbul textile machinery exhibitors",
                "Istanbul textile fair exhibitor list",
                "Turkey textile fair exhibitors",
            ],
            "base_url": "",
        },
        {
            "name": "Premiere Vision",
            "search_queries": [
                "Premiere Vision exhibitor list",
                "Premiere Vision Paris textile",
            ],
            "base_url": "https://www.premierevision.com/",
        },
        {
            "name": "Texworld",
            "search_queries": [
                "Texworld Paris exhibitor list",
                "Texworld textile exhibitors",
            ],
            "base_url": "",
        },
    ]
    
    # Patterns to find exhibitor data
    COMPANY_PATTERNS = [
        r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*(?:\s+(?:Textile|Textil|S\.A\.|Ltd|GmbH|SpA|Inc|A\.S\.|A\.Ş\.)))",
        r"([A-Z][a-zA-Z]{2,}(?:\s+[A-Za-z]+){0,4})\s*[\|\-\,]\s*(?:Hall|Stand|Booth)",
    ]

    def __init__(self, brave_api_key=None, settings=None, policies=None):
        self.client = HttpClient(settings=settings, policies=policies)
        self.brave_api_key = brave_api_key or os.environ.get("BRAVE_API_KEY") or os.environ.get("Brave_API_KEY")
        
    def harvest(self):
        """Main harvest method - returns list of leads."""
        all_leads = []
        
        for fair in self.FAIRS:
            logger.info(f"Collecting exhibitors from {fair['name']}...")
            
            # 1. Search for exhibitor lists via Brave
            if self.brave_api_key:
                leads = self._search_fair_exhibitors(fair)
                all_leads.extend(leads)
                logger.info(f"  {fair['name']}: {len(leads)} exhibitors")
            
            # 2. Direct scrape if URL known
            if fair.get("base_url"):
                direct_leads = self._scrape_fair_site(fair)
                all_leads.extend(direct_leads)
            
            time.sleep(1)  # Rate limiting
        
        # Dedupe
        seen = set()
        unique = []
        for lead in all_leads:
            key = lead.get("company", "").lower().strip()
            if key and key not in seen:
                seen.add(key)
                unique.append(lead)
        
        logger.info(f"Fair Exhibitor Collector: {len(unique)} unique leads")
        return unique
    
    def _search_fair_exhibitors(self, fair):
        """Search Brave for exhibitor lists."""
        if not self.brave_api_key:
            return []
        
        leads = []
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": self.brave_api_key
        }
        
        for query in fair.get("search_queries", [])[:3]:
            try:
                url = f"https://api.search.brave.com/res/v1/web/search?q={query}&count=10"
                resp = requests.get(url, headers=headers, timeout=15)
                
                if resp.status_code == 429:
                    logger.warning("Rate limit, waiting...")
                    time.sleep(5)
                    continue
                    
                if resp.status_code != 200:
                    continue
                
                data = resp.json()
                results = data.get("web", {}).get("results", [])
                
                for result in results:
                    page_url = result.get("url", "")
                    title = result.get("title", "")
                    
                    # Skip non-exhibitor pages
                    if not any(kw in title.lower() or kw in page_url.lower() 
                               for kw in ["exhibitor", "aussteller", "exposant", "list", "directory"]):
                        continue
                    
                    # Fetch and extract exhibitors
                    exhibitors = self._extract_exhibitors(page_url, fair["name"])
                    leads.extend(exhibitors)
                
                time.sleep(1)
                
            except Exception as e:
                logger.warning(f"Brave search error: {e}")
                continue
        
        return leads
    
    def _scrape_fair_site(self, fair):
        """Direct scrape of fair website."""
        leads = []
        base_url = fair.get("base_url", "")
        if not base_url:
            return leads
        
        # Common exhibitor list paths
        paths = [
            "/en/exhibitors/",
            "/exhibitors/",
            "/exhibitor-list/",
            "/aussteller/",
            "/en/visit/exhibitor-search/",
        ]
        
        for path in paths:
            url = urljoin(base_url, path)
            try:
                html = self.client.get(url)
                if not html or len(html) < 1000:
                    continue
                
                soup = BeautifulSoup(html, "html.parser")
                
                # Find exhibitor entries
                for item in soup.find_all(["div", "li", "tr", "article"], 
                                          class_=re.compile(r"exhibitor|company|firma|aussteller")):
                    name = item.get_text(strip=True)[:100]
                    
                    if self._is_valid_exhibitor(name):
                        # Try to find country
                        country = self._extract_country(item.get_text())
                        
                        leads.append({
                            "company": name,
                            "country": country,
                            "source": url,
                            "source_type": "fair",
                            "source_name": fair["name"],
                            "context": f"Exhibitor at {fair['name']} trade fair",
                        })
                
            except Exception as e:
                logger.debug(f"Error scraping {url}: {e}")
        
        return leads
    
    def _extract_exhibitors(self, page_url, fair_name):
        """Extract exhibitor names from a page."""
        leads = []
        
        try:
            html = self.client.get(page_url)
            if not html:
                return leads
            
            # Try trafilatura first
            text = trafilatura.extract(html) or ""
            if not text:
                soup = BeautifulSoup(html, "html.parser")
                text = soup.get_text(separator="\n", strip=True)
            
            # Extract company names
            for pattern in self.COMPANY_PATTERNS:
                matches = re.findall(pattern, text)
                for match in matches[:50]:  # Limit per page
                    if isinstance(match, tuple):
                        match = match[0]
                    
                    if self._is_valid_exhibitor(match):
                        leads.append({
                            "company": match.strip(),
                            "source": page_url,
                            "source_type": "fair",
                            "source_name": fair_name,
                            "context": f"Exhibitor at {fair_name}",
                        })
            
        except Exception as e:
            logger.debug(f"Error extracting from {page_url}: {e}")
        
        return leads
    
    def _is_valid_exhibitor(self, name):
        """Check if name is a valid exhibitor."""
        if not name or len(name) < 3:
            return False
        if len(name) > 100:
            return False
        
        # Skip navigation/generic terms
        skip_terms = [
            "exhibitor", "aussteller", "hall", "stand", "booth",
            "search", "filter", "more", "view", "click", "next",
            "previous", "page", "home", "contact", "about"
        ]
        name_l = name.lower()
        if name_l in skip_terms:
            return False
        
        # Must have at least 2 characters that are letters
        if sum(1 for c in name if c.isalpha()) < 3:
            return False
        
        return True
    
    def _extract_country(self, text):
        """Extract country from exhibitor entry."""
        countries = {
            "Turkey": ["turkey", "türkiye", "tr"],
            "Brazil": ["brazil", "brasil", "br"],
            "Egypt": ["egypt", "eg"],
            "Germany": ["germany", "deutschland", "de"],
            "Italy": ["italy", "italia", "it"],
            "China": ["china", "cn"],
            "India": ["india", "in"],
            "Pakistan": ["pakistan", "pk"],
            "Morocco": ["morocco", "ma"],
        }
        
        text_l = text.lower()
        for country, keywords in countries.items():
            if any(kw in text_l for kw in keywords):
                return country
        
        return ""
