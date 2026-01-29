#!/usr/bin/env python3
"""
Competitor Customer Intelligence

Finds customers of competitors (Interspare & XTY/Elinmac) through:
1. Direct website scraping (news, references, case studies)
2. Brave Search API for customer mentions
3. Google search patterns for "bei [competitor]" mentions

These are HIGH VALUE leads - if they buy from competitors,
they need spare parts!
"""

import re
import os
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
from pathlib import Path

from bs4 import BeautifulSoup
import trafilatura
import requests
import yaml

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache
from src.utils.evidence import record_evidence

logger = get_logger(__name__)


class CompetitorCustomerIntel:
    """
    Hunts for customers of main competitors (Interspare & XTY/Elinmac).
    
    Strategy:
    1. Scrape competitor websites for reference/news pages
    2. Search Brave for "[competitor] + customer/reference" mentions
    3. Extract company names from context
    4. Validate and enrich
    """
    
    # Company name extraction patterns
    COMPANY_PATTERNS = [
        # German patterns
        r"(?:bei|für|an|Kunde|Projekt bei)\s+([A-Z][a-zA-ZäöüÄÖÜß]+(?:\s+[A-Z][a-zA-ZäöüÄÖÜß]+)*(?:\s+(?:GmbH|AG|KG|S\.A\.|Ltd|SpA|A\.Ş\.))?)",
        # English patterns
        r"(?:at|for|customer|client|project at)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*(?:\s+(?:Ltd|Inc|LLC|Corp|S\.A\.|SpA))?)",
        # Generic company patterns
        r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\s+(?:Textile|Textil|Mills?|Fabrik|Factory|Dyeing|Finishing|Weaving))",
        r"([A-Z][a-zA-Z]{2,}(?:\s+[A-Za-z]+){1,3}\s+(?:S\.A\.E?|S\.A\.|A\.S\.|A\.Ş\.|Ltd|LLC|GmbH|SpA|S\.p\.A\.))",
    ]
    
    # Keywords indicating customer relationship
    CUSTOMER_SIGNALS = {
        "de": ["kunde", "kunden", "referenz", "referenzen", "projekt", "projekte", 
               "lieferung", "geliefert", "bei", "für", "installation", "modernisierung"],
        "en": ["customer", "client", "reference", "project", "delivered", "supplied",
               "installation", "at", "for", "retrofit", "modernization"],
        "tr": ["müşteri", "referans", "proje", "kurulum", "teslim"],
    }

    def __init__(self, config_path=None, brave_api_key=None, evidence_path=None):
        self.brave_api_key = brave_api_key or os.environ.get("BRAVE_API_KEY") or os.environ.get("Brave_API_KEY")
        self.evidence_path = evidence_path or "outputs/evidence/evidence_log.csv"
        
        # Load products config
        config_path = config_path or Path(__file__).parent.parent.parent / "config" / "products.yaml"
        with open(config_path, "r", encoding="utf-8") as f:
            self.products_config = yaml.safe_load(f)
        
        self.competitors = self.products_config.get("competitors", {})
        self.client = HttpClient()
        self.found_customers = []
        
    def harvest(self):
        """Main harvest method - find all competitor customers."""
        all_leads = []
        
        for competitor_key, competitor_data in self.competitors.items():
            logger.info(f"🔍 Hunting {competitor_data['name']} customers...")
            
            # 1. Search via Brave API
            if self.brave_api_key:
                brave_leads = self._search_brave(competitor_key, competitor_data)
                all_leads.extend(brave_leads)
                logger.info(f"  Brave search: {len(brave_leads)} customer mentions")
            
            # 2. Direct website scrape
            site_leads = self._scrape_competitor_site(competitor_key, competitor_data)
            all_leads.extend(site_leads)
            logger.info(f"  Website scrape: {len(site_leads)} references")
            
            time.sleep(2)  # Rate limiting between competitors
        
        # Dedupe by company name
        unique_leads = self._dedupe_leads(all_leads)
        
        logger.info(f"✅ Competitor Customer Intel: {len(unique_leads)} unique customers found")
        return unique_leads
    
    def _search_brave(self, competitor_key, competitor_data):
        """Search Brave for competitor customer mentions."""
        if not self.brave_api_key:
            return []
        
        leads = []
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": self.brave_api_key
        }
        
        search_keywords = competitor_data.get("search_keywords", [])
        
        for query in search_keywords[:6]:  # Limit queries
            try:
                url = f"https://api.search.brave.com/res/v1/web/search?q={query}&count=10"
                resp = requests.get(url, headers=headers, timeout=15)
                
                if resp.status_code == 429:
                    logger.warning("Brave rate limit, waiting...")
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
                    
                    # Skip competitor's own site in results
                    if competitor_data.get("url", "") in page_url:
                        # But still extract from their pages
                        pass
                    
                    # Extract company names from title + description
                    text = f"{title} {description}"
                    companies = self._extract_companies(text)
                    
                    for company in companies:
                        if self._is_valid_customer(company, competitor_data):
                            country = self._extract_country(text)
                            
                            leads.append({
                                "company": company,
                                "country": country,
                                "source": page_url,
                                "source_type": "competitor_customer",
                                "source_name": f"{competitor_data['name']} Customer",
                                "context": f"Found via {competitor_data['name']} search: {title[:100]}",
                                "competitor_reference": competitor_data['name'],
                                "evidence_snippet": description[:200],
                            })
                            
                            # Record evidence
                            record_evidence(self.evidence_path, {
                                "source_type": "competitor_customer",
                                "source_name": competitor_data['name'],
                                "url": page_url,
                                "title": title,
                                "snippet": description[:200],
                                "company_found": company,
                                "fetched_at": datetime.utcnow().isoformat(),
                            })
                
                time.sleep(1)
                
            except Exception as e:
                logger.warning(f"Brave search error: {e}")
                continue
        
        return leads
    
    def _scrape_competitor_site(self, competitor_key, competitor_data):
        """Scrape competitor website for reference pages."""
        leads = []
        base_url = competitor_data.get("url", "")
        if not base_url:
            return leads
        
        reference_pages = competitor_data.get("reference_pages", [
            "/aktuelles", "/news", "/referenzen", "/projekte", "/case-studies"
        ])
        
        for path in reference_pages:
            url = urljoin(base_url, path)
            try:
                html = self.client.get(url)
                if not html:
                    continue
                
                # Save raw content
                content_hash = save_text_cache(url, html)
                
                # Extract text
                text = trafilatura.extract(html) or ""
                if not text:
                    soup = BeautifulSoup(html, "html.parser")
                    text = soup.get_text(separator="\n", strip=True)
                
                # Find customer mentions
                companies = self._extract_companies(text)
                
                for company in companies:
                    if self._is_valid_customer(company, competitor_data):
                        country = self._extract_country(text)
                        context_snippet = self._get_context_snippet(text, company)
                        
                        leads.append({
                            "company": company,
                            "country": country,
                            "source": url,
                            "source_type": "competitor_customer",
                            "source_name": f"{competitor_data['name']} References",
                            "context": f"Referenced on {competitor_data['name']} website: {context_snippet}",
                            "competitor_reference": competitor_data['name'],
                            "evidence_hash": content_hash,
                        })
                        
                        # Record evidence
                        record_evidence(self.evidence_path, {
                            "source_type": "competitor_customer",
                            "source_name": competitor_data['name'],
                            "url": url,
                            "company_found": company,
                            "snippet": context_snippet,
                            "content_hash": content_hash,
                            "fetched_at": datetime.utcnow().isoformat(),
                        })
                
            except Exception as e:
                logger.debug(f"Error scraping {url}: {e}")
                continue
        
        return leads
    
    def _extract_companies(self, text):
        """Extract company names from text."""
        companies = set()
        
        for pattern in self.COMPANY_PATTERNS:
            try:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0] if match[0] else (match[1] if len(match) > 1 else "")
                    if match and len(match) > 3 and len(match) < 100:
                        companies.add(match.strip())
            except:
                continue
        
        return list(companies)
    
    def _is_valid_customer(self, company, competitor_data):
        """Check if extracted company name is a valid customer."""
        if not company or len(company) < 4:
            return False
        
        company_l = company.lower()
        
        # Skip competitor names
        competitor_name = competitor_data.get("name", "").lower()
        if competitor_name and competitor_name in company_l:
            return False
        
        # Skip common non-company terms
        skip_terms = [
            "interspare", "elinmac", "xty", "brückner", "brueckner", "monforts",
            "textile machinery", "spare parts", "the company", "our customer",
            "project", "news", "aktuelles", "referenz", "case study"
        ]
        if company_l in skip_terms or any(term == company_l for term in skip_terms):
            return False
        
        return True
    
    def _extract_country(self, text):
        """Extract country from context."""
        countries = {
            "Turkey": ["turkey", "türkiye", "turkish", "istanbul", "bursa", "denizli", "gaziantep"],
            "Brazil": ["brazil", "brasil", "brazilian", "são paulo", "sao paulo"],
            "Egypt": ["egypt", "egyptian", "cairo", "alexandria"],
            "Morocco": ["morocco", "moroccan", "casablanca", "tangier"],
            "Tunisia": ["tunisia", "tunisian", "tunis", "sfax"],
            "India": ["india", "indian", "mumbai", "ahmedabad", "tirupur"],
            "Pakistan": ["pakistan", "pakistani", "karachi", "lahore", "faisalabad"],
            "Bangladesh": ["bangladesh", "dhaka", "chittagong"],
            "Vietnam": ["vietnam", "vietnamese", "ho chi minh"],
            "Indonesia": ["indonesia", "indonesian", "jakarta", "bandung"],
            "Argentina": ["argentina", "argentinian", "buenos aires"],
            "Colombia": ["colombia", "colombian", "bogota", "medellin"],
            "Peru": ["peru", "peruvian", "lima"],
            "Germany": ["germany", "german", "deutschland"],
            "Ghana": ["ghana", "ghanaian", "accra"],
            "Ivory Coast": ["ivory coast", "côte d'ivoire", "cote d'ivoire"],
        }
        
        text_l = text.lower()
        for country, keywords in countries.items():
            if any(kw in text_l for kw in keywords):
                return country
        
        return ""
    
    def _get_context_snippet(self, text, company):
        """Get context around company mention."""
        text_l = text.lower()
        company_l = company.lower()
        
        pos = text_l.find(company_l)
        if pos >= 0:
            start = max(0, pos - 100)
            end = min(len(text), pos + len(company) + 100)
            return text[start:end].replace("\n", " ").strip()
        
        return ""
    
    def _dedupe_leads(self, leads):
        """Remove duplicate leads by company name."""
        seen = set()
        unique = []
        
        for lead in leads:
            key = lead.get("company", "").lower().strip()
            if key and key not in seen:
                seen.add(key)
                unique.append(lead)
        
        return unique


# Standalone test
if __name__ == "__main__":
    hunter = CompetitorCustomerIntel()
    leads = hunter.harvest()
    
    print(f"\n=== Found {len(leads)} competitor customers ===\n")
    for lead in leads[:20]:
        print(f"  {lead['company']:<40} | {lead.get('country', 'N/A'):<15} | {lead['competitor_reference']}")
