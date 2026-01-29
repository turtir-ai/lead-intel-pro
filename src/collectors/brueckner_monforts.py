#!/usr/bin/env python3
"""
Brückner & Monforts Customer Hunter

Finds customers of main OEM competitors using:
1. Direct website scraping (reference/news pages)
2. Brave Search for customer mentions
3. Entity extraction from content

These are the PRIMARY targets - companies using Brückner/Monforts
machines need spare parts!
"""

import re
import os
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
from pathlib import Path

from bs4 import BeautifulSoup
import trafilatura

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


class OEMCustomerHunter:
    """
    Hunts for customers of Brückner and Monforts from multiple sources.
    """
    
    # Keywords indicating customer relationship
    CUSTOMER_SIGNALS = [
        # German
        "kunde", "kunden", "referenz", "referenzen", "projekt", "projekte",
        "installation", "installiert", "lieferung", "geliefert", "auftrag",
        # English  
        "customer", "client", "reference", "project", "installation",
        "delivered", "supplied", "order", "commissioned", "equipped",
        # Spanish
        "cliente", "clientes", "proyecto", "instalacion", "entrega",
        # Portuguese
        "cliente", "projeto", "instalacao", "entrega",
        # Turkish
        "müşteri", "referans", "proje", "kurulum", "teslim",
    ]
    
    # Company name patterns (textile mills)
    COMPANY_PATTERNS = [
        r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\s+(?:Textile|Textil|Mills?|Fabrik|Factory|S\.A\.E?|S\.A\.|A\.S\.|Ltd|LLC|GmbH|SpA|S\.p\.A\.|Inc))",
        r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\s+(?:Dyeing|Finishing|Weaving|Spinning|Knitting))",
        r"((?:Delta|Globe|Nile|Egyptian|Cairo|Istanbul|Bursa|Denizli|Sao Paulo)\s+[A-Za-z]+(?:\s+[A-Za-z]+)*)",
    ]
    
    # Known OEMs and their product keywords
    OEMS = {
        "Brückner": {
            "url": "https://www.brueckner-textile.com/",
            "products": ["stenter", "finishing", "coating", "drying", "heat setting"],
            "search_queries": [
                '"Brückner" textile customer reference',
                '"Brückner" stenter installation',
                '"Brückner" finishing line delivered',
                '"Brückner Textile" mill project',
                'site:brueckner-textile.com reference',
                '"equipped with Brückner"',
                '"Brückner stenter" installed',
            ]
        },
        "Monforts": {
            "url": "https://www.monforts.de/",
            "products": ["montex", "thermex", "stenter", "dyeing", "finishing"],
            "search_queries": [
                '"Monforts" textile customer reference',
                '"Monforts" stenter installation',
                '"Montex" delivered textile',
                '"Thermex" textile mill',
                'site:monforts.de reference',
                '"equipped with Monforts"',
                '"Monforts stenter" installed',
            ]
        }
    }

    def __init__(self, brave_api_key=None, settings=None, policies=None):
        self.client = HttpClient(settings=settings, policies=policies)
        self.brave_api_key = brave_api_key or os.environ.get("BRAVE_API_KEY") or os.environ.get("Brave_API_KEY")
        self.customers = []
        
    def harvest(self):
        """Main harvest method - returns list of leads."""
        all_leads = []
        
        for oem_name, oem_config in self.OEMS.items():
            logger.info(f"Hunting {oem_name} customers...")
            
            # 1. Search via Brave API
            if self.brave_api_key:
                leads = self._search_brave(oem_name, oem_config)
                all_leads.extend(leads)
                logger.info(f"  Brave search: {len(leads)} mentions")
            
            # 2. Direct website scrape
            site_leads = self._scrape_website(oem_name, oem_config)
            all_leads.extend(site_leads)
            logger.info(f"  Website scrape: {len(site_leads)} references")
        
        # Dedupe by company name
        seen = set()
        unique_leads = []
        for lead in all_leads:
            key = lead.get("company", "").lower().strip()
            if key and key not in seen:
                seen.add(key)
                unique_leads.append(lead)
        
        logger.info(f"OEM Customer Hunter: found {len(unique_leads)} unique customers")
        return unique_leads
    
    def _search_brave(self, oem_name, oem_config):
        """Search Brave for customer mentions."""
        if not self.brave_api_key:
            return []
        
        leads = []
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": self.brave_api_key
        }
        
        for query in oem_config.get("search_queries", [])[:5]:  # Limit to avoid rate limits
            try:
                url = f"https://api.search.brave.com/res/v1/web/search?q={query}&count=10"
                import requests
                resp = requests.get(url, headers=headers, timeout=15)
                
                if resp.status_code == 429:
                    logger.warning("Brave API rate limit, waiting...")
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
                    
                    # Extract company names from title/description
                    companies = self._extract_companies(f"{title} {description}")
                    
                    for company in companies:
                        if self._is_valid_company(company, oem_name):
                            leads.append({
                                "company": company,
                                "source": page_url,
                                "source_type": "oem_customer",
                                "source_name": f"{oem_name} Customer Search",
                                "context": f"Found via {oem_name} search: {title}. {description}",
                                "oem_reference": oem_name,
                            })
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Brave search error: {e}")
                continue
        
        return leads
    
    def _scrape_website(self, oem_name, oem_config):
        """Scrape OEM website for reference pages."""
        leads = []
        base_url = oem_config["url"]
        
        # Reference page paths to check
        paths = [
            "/en/references/",
            "/references/",
            "/en/news/",
            "/news/",
            "/en/case-studies/",
            "/referenzen/",
            "/aktuelles/",
        ]
        
        for path in paths:
            url = urljoin(base_url, path)
            try:
                html = self.client.get(url)
                if not html:
                    continue
                
                # Extract text
                text = trafilatura.extract(html) or ""
                if not text:
                    soup = BeautifulSoup(html, "html.parser")
                    text = soup.get_text(separator="\n", strip=True)
                
                # Find companies mentioned
                companies = self._extract_companies(text)
                
                for company in companies:
                    if self._is_valid_company(company, oem_name):
                        # Try to find country from context
                        country = self._extract_country(text, company)
                        
                        leads.append({
                            "company": company,
                            "country": country,
                            "source": url,
                            "source_type": "oem_customer",
                            "source_name": f"{oem_name} References",
                            "context": f"Referenced on {oem_name} website",
                            "oem_reference": oem_name,
                        })
                
            except Exception as e:
                logger.debug(f"Error scraping {url}: {e}")
                continue
        
        return leads
    
    def _extract_companies(self, text):
        """Extract company names from text."""
        companies = set()
        
        for pattern in self.COMPANY_PATTERNS:
            matches = re.findall(pattern, text)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                if len(match) > 3 and len(match) < 100:
                    companies.add(match.strip())
        
        return list(companies)
    
    def _is_valid_company(self, company, oem_name):
        """Check if company name is valid (not the OEM itself)."""
        company_l = company.lower()
        
        # Skip OEM names
        if oem_name.lower() in company_l:
            return False
        if "brückner" in company_l or "brueckner" in company_l:
            return False
        if "monforts" in company_l:
            return False
        
        # Skip generic terms
        skip_terms = ["textile machinery", "textile machines", "the company", "the mill"]
        if any(term in company_l for term in skip_terms):
            return False
        
        return True
    
    def _extract_country(self, text, company):
        """Try to extract country from context."""
        countries = {
            "Turkey": ["turkey", "türkiye", "turkish", "istanbul", "bursa", "denizli", "gaziantep"],
            "Brazil": ["brazil", "brasil", "brazilian", "são paulo", "sao paulo"],
            "Egypt": ["egypt", "egyptian", "cairo", "alexandria"],
            "India": ["india", "indian", "mumbai", "ahmedabad", "tirupur"],
            "Pakistan": ["pakistan", "pakistani", "karachi", "lahore", "faisalabad"],
            "Bangladesh": ["bangladesh", "dhaka", "chittagong"],
            "Vietnam": ["vietnam", "vietnamese", "ho chi minh"],
            "Indonesia": ["indonesia", "indonesian", "jakarta", "bandung"],
            "Morocco": ["morocco", "moroccan", "casablanca", "tangier"],
            "Tunisia": ["tunisia", "tunisian", "tunis", "sousse"],
            "Argentina": ["argentina", "argentinian", "buenos aires"],
            "Colombia": ["colombia", "colombian", "bogota", "medellin"],
            "Peru": ["peru", "peruvian", "lima"],
            "Mexico": ["mexico", "mexican"],
        }
        
        text_l = text.lower()
        
        # Look for country mentions near company name
        company_pos = text_l.find(company.lower())
        if company_pos >= 0:
            context = text_l[max(0, company_pos-200):company_pos+200]
        else:
            context = text_l
        
        for country, keywords in countries.items():
            if any(kw in context for kw in keywords):
                return country
        
        return ""
