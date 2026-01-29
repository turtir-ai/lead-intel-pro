#!/usr/bin/env python3
"""
Turkey Textile Collector

Collects textile companies from Turkish sources:
1. ITKIB (Istanbul Textile and Apparel Exporters' Association)
2. UTİB (Uludağ Textile Exporters' Association)
3. BTSO (Bursa Chamber of Commerce)
4. Turkish textile directories
"""

import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TurkeyTextileCollector:
    """
    Collects Turkish textile companies from industry associations.
    Turkey is the #1 priority market for textile machinery spare parts!
    """
    
    # Turkish textile associations and directories
    SOURCES = [
        {
            "name": "ITKIB Exporters",
            "base_url": "https://www.itkib.org.tr/",
            "search_url": "https://www.itkib.org.tr/Home/UyeBul",
            "type": "association"
        },
        {
            "name": "UTİB Members", 
            "base_url": "https://www.utib.org.tr/",
            "type": "association"
        },
    ]
    
    # Cities with major textile industry
    TEXTILE_CITIES = [
        "istanbul", "bursa", "denizli", "gaziantep", "kahramanmaraş",
        "adana", "kayseri", "uşak", "tekirdağ", "çorlu"
    ]
    
    # Keywords for textile finishing companies (target customers)
    TARGET_KEYWORDS = [
        "boya", "apre", "terbiye", "finishing", "dyeing",
        "dokuma", "örme", "iplik", "kumaş", "tekstil",
        "ramöz", "stenter", "kasar", "merserizasyon"
    ]

    def __init__(self, brave_api_key=None, settings=None, policies=None):
        self.client = HttpClient(settings=settings, policies=policies)
        self.brave_api_key = brave_api_key or os.environ.get("BRAVE_API_KEY") or os.environ.get("Brave_API_KEY")
        
    def harvest(self):
        """Main harvest method - returns list of leads."""
        all_leads = []
        
        # 1. Search via Brave for Turkish textile companies
        if self.brave_api_key:
            brave_leads = self._search_brave_turkey()
            all_leads.extend(brave_leads)
            logger.info(f"Turkey Brave search: {len(brave_leads)} leads")
        
        # 2. Known Turkish textile directories
        dir_leads = self._scrape_directories()
        all_leads.extend(dir_leads)
        logger.info(f"Turkey directories: {len(dir_leads)} leads")
        
        # Dedupe
        seen = set()
        unique = []
        for lead in all_leads:
            key = lead.get("company", "").lower().strip()
            if key and key not in seen:
                seen.add(key)
                unique.append(lead)
        
        logger.info(f"Turkey Collector: {len(unique)} unique leads")
        return unique
    
    def _search_brave_turkey(self):
        """Search Brave for Turkish textile companies."""
        if not self.brave_api_key:
            return []
        
        leads = []
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": self.brave_api_key
        }
        
        queries = [
            # Turkish textile associations
            '"ITKIB" tekstil firma listesi',
            '"UTİB" üye listesi tekstil',
            'Bursa tekstil boyahane listesi',
            'Denizli tekstil terbiye firmaları',
            'Istanbul tekstil finishing companies',
            'Turkey textile dyeing finishing company list',
            '"Türkiye tekstil" apre terbiye fabrika',
            'Turkish textile mills dyeing finishing',
            # OEKO-TEX in Turkey
            'OEKO-TEX certified Turkey textile',
            'GOTS certified Turkey textile factory',
            # Trade associations
            'TİM tekstil ihracatçılar listesi',
        ]
        
        for query in queries[:8]:  # Limit queries
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
                    title = result.get("title", "")
                    description = result.get("description", "")
                    page_url = result.get("url", "")
                    
                    # Extract companies from page
                    page_leads = self._extract_from_page(page_url, title, description)
                    leads.extend(page_leads)
                
                time.sleep(1)
                
            except Exception as e:
                logger.warning(f"Brave search error: {e}")
                continue
        
        return leads
    
    def _scrape_directories(self):
        """Scrape known Turkish textile directories."""
        leads = []
        
        # These are example directories - actual implementation would
        # need specific page structures
        directories = [
            ("https://www.tekstilportal.com/firmalar", "Tekstil Portal"),
            ("https://www.tekstilrehberi.com/", "Tekstil Rehberi"),
        ]
        
        for url, source_name in directories:
            try:
                html = self.client.get(url)
                if not html:
                    continue
                
                soup = BeautifulSoup(html, "html.parser")
                
                # Find company listings
                for item in soup.find_all(["div", "li", "article"], class_=re.compile(r"company|firma|uye|member|listing")):
                    company_name = item.get_text(strip=True)[:100]
                    
                    # Find links
                    link = item.find("a", href=True)
                    website = link["href"] if link else ""
                    
                    if company_name and self._is_valid_company(company_name):
                        leads.append({
                            "company": company_name,
                            "country": "Turkey",
                            "website": website if website.startswith("http") else "",
                            "source": url,
                            "source_type": "directory",
                            "source_name": source_name,
                            "context": f"Turkish textile company from {source_name}",
                        })
                
            except Exception as e:
                logger.debug(f"Error scraping {url}: {e}")
                continue
        
        return leads
    
    def _extract_from_page(self, page_url, title, description):
        """Extract company information from a search result page."""
        leads = []
        
        # Check if it's a directory/list page
        if not any(kw in title.lower() or kw in description.lower() 
                   for kw in ["liste", "list", "üye", "member", "firma", "şirket", "company"]):
            return leads
        
        try:
            html = self.client.get(page_url)
            if not html:
                return leads
            
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text(separator="\n", strip=True)
            
            # Find company patterns
            patterns = [
                r"([A-ZÇĞİÖŞÜ][a-zçğıöşü]+(?:\s+[A-ZÇĞİÖŞÜa-zçğıöşü]+)*\s+(?:Tekstil|Boya|Apre|Terbiye|A\.Ş\.|Ltd|San\.?|Tic\.?))",
                r"([A-Z][a-z]+(?:\s+[A-Za-z]+)*\s+(?:Textile|Dyeing|Finishing|A\.S\.|LTD))",
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, text)
                for match in matches[:20]:  # Limit per page
                    if self._is_valid_company(match):
                        leads.append({
                            "company": match.strip(),
                            "country": "Turkey",
                            "source": page_url,
                            "source_type": "directory",
                            "source_name": "Brave Search",
                            "context": f"From: {title}",
                        })
            
        except Exception as e:
            logger.debug(f"Error extracting from {page_url}: {e}")
        
        return leads
    
    def _is_valid_company(self, name):
        """Check if company name is valid."""
        if not name or len(name) < 4:
            return False
        
        # Skip generic terms
        skip_terms = ["tekstil", "textile", "turkey", "türkiye", "list", "liste", "member", "üye"]
        name_l = name.lower()
        
        if name_l in skip_terms:
            return False
        if len(name) > 100:
            return False
        
        return True
