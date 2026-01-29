#!/usr/bin/env python3
"""
Precision Customer Finder - Nokta Atışı Müşteri Bulucu

16 ürün için tam hedefli müşteri arama:
1. Ürün bazlı Brave Search (çok dilli)
2. OEM müşteri referansları
3. Rakip müşteri tespiti
4. Ülke + sektör kesişimi

Her ürün için ideal müşteri profili:
- OEM ekipmanı olan (Brückner, Monforts, Artos, Krantz)
- Finishing/dyeing işi yapan
- Hedef ülkede olan (TUR, BRA, EGY, MAR, ARG)
"""

import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime
from dataclasses import dataclass

import requests
import yaml

from src.utils.logger import get_logger
from src.utils.http_client import HttpClient
from src.utils.evidence import record_evidence

logger = get_logger(__name__)

# Evidence path
EVIDENCE_PATH = Path(__file__).parent.parent.parent / "outputs" / "evidence" / "evidence_log.csv"


@dataclass
class ProductMatch:
    """Ürün-müşteri eşleşmesi."""
    product_id: str
    product_name: str
    brand: str
    hs_code: str
    customer_company: str
    customer_country: str
    match_reason: str  # "oem_equipment", "competitor_customer", "sector_fit"
    confidence: float  # 0-1
    source_url: str
    context: str


class PrecisionCustomerFinder:
    """
    Nokta atışı müşteri bulucu.
    
    Her ürün için:
    1. O ürünün OEM'i olan firmaları bul
    2. O OEM'in müşterilerini bul
    3. Rakiplerin müşterilerini bul
    4. Sonuçları filtrele ve skorla
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        self.api_key = os.environ.get("BRAVE_API_KEY") or os.environ.get("Brave_API_KEY")
        self.http_client = HttpClient()
        
        # Load config
        config_path = config_path or Path(__file__).parent.parent.parent / "config" / "products.yaml"
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        
        self.products = self.config.get("products", [])
        self.brand_keywords = self.config.get("brand_keywords", [])
        self.part_keywords = self.config.get("part_keywords", {})
        self.competitors = self.config.get("competitors", {})
        
        # Target countries
        self.target_countries = [
            "Turkey", "Türkiye", "Brazil", "Brasil", "Egypt", "Morocco",
            "Tunisia", "Argentina", "Colombia", "Peru", "India", "Pakistan", "Bangladesh"
        ]
        
        # Stats
        self.stats = {
            "queries_made": 0,
            "urls_found": 0,
            "companies_extracted": 0,
            "matches_found": 0
        }
    
    def find_customers_for_product(self, product: Dict, max_queries: int = 5) -> List[Dict]:
        """
        Belirli bir ürün için potansiyel müşterileri bul.
        
        Args:
            product: Ürün config dict'i
            max_queries: Maksimum arama sayısı
        """
        product_id = product.get("id", "")
        product_name = product.get("name", "")
        brand = product.get("brand", "")
        search_keywords = product.get("search_keywords", [])
        
        logger.info(f"🎯 Finding customers for: {product_name}")
        
        all_leads = []
        seen_companies = set()
        
        # 1. Product-specific searches
        for query in search_keywords[:max_queries]:
            results = self._brave_search(query)
            for r in results:
                companies = self._extract_companies_from_result(r, product)
                for company in companies:
                    name = company.get("company", "").lower()
                    if name and name not in seen_companies:
                        seen_companies.add(name)
                        company["product_id"] = product_id
                        company["product_name"] = product_name
                        company["brand"] = brand
                        company["match_type"] = "product_search"
                        all_leads.append(company)
            
            time.sleep(0.5)
        
        # 2. OEM customer searches
        oem_leads = self._find_oem_customers(brand, product_name)
        for lead in oem_leads:
            name = lead.get("company", "").lower()
            if name and name not in seen_companies:
                seen_companies.add(name)
                lead["product_id"] = product_id
                lead["match_type"] = "oem_customer"
                all_leads.append(lead)
        
        logger.info(f"  → {len(all_leads)} potential customers found")
        self.stats["matches_found"] += len(all_leads)
        
        return all_leads
    
    def find_all_customers(self, max_products: int = None) -> List[Dict]:
        """Tüm ürünler için müşteri bul."""
        all_leads = []
        products = self.products[:max_products] if max_products else self.products
        
        for product in products:
            leads = self.find_customers_for_product(product)
            all_leads.extend(leads)
        
        # Dedupe by company name
        seen = set()
        unique_leads = []
        for lead in all_leads:
            name = lead.get("company", "").lower()
            if name and name not in seen:
                seen.add(name)
                unique_leads.append(lead)
        
        logger.info(f"\n📊 Total: {len(unique_leads)} unique potential customers")
        return unique_leads
    
    def find_customers_multilang(self, 
                                  languages: List[str] = None,
                                  countries: List[str] = None) -> List[Dict]:
        """
        Çok dilli arama ile müşteri bul.
        
        Her dil için:
        - Sektör keyword'leri
        - OEM referansları
        - Ülke + sektör kesişimi
        """
        languages = languages or ["de", "en", "tr", "es", "pt"]
        countries = countries or ["Turkey", "Brazil", "Egypt", "Morocco", "Argentina"]
        
        all_leads = []
        seen_companies = set()
        
        for lang in languages:
            keywords = self.part_keywords.get(lang, []) or self.part_keywords.get("english", [])
            
            for country in countries:
                # Build queries
                queries = self._build_country_queries(keywords, country, lang)
                
                for query in queries[:3]:  # Max 3 per country/lang
                    results = self._brave_search(query)
                    for r in results:
                        companies = self._extract_companies_from_result(r, {})
                        for company in companies:
                            # Filter by target country
                            company_country = company.get("country", "")
                            if company_country and any(c.lower() in company_country.lower() for c in countries):
                                name = company.get("company", "").lower()
                                if name and name not in seen_companies:
                                    seen_companies.add(name)
                                    company["search_lang"] = lang
                                    company["match_type"] = "multilang_search"
                                    all_leads.append(company)
                    
                    time.sleep(0.3)
        
        logger.info(f"Multi-lang search: {len(all_leads)} leads from {len(languages)} languages")
        return all_leads
    
    def _build_country_queries(self, keywords: List[str], country: str, lang: str) -> List[str]:
        """Ülke + keyword query'leri oluştur."""
        queries = []
        
        # Get templates from config
        templates = self.config.get("search_templates", {}).get("country_sector", {}).get(lang, [])
        
        if templates:
            for template in templates[:3]:
                queries.append(template.format(country=country))
        else:
            # Fallback
            for kw in keywords[:3]:
                queries.append(f'"{kw}" {country}')
        
        return queries
    
    def _find_oem_customers(self, brand: str, product_name: str) -> List[Dict]:
        """OEM müşterilerini bul - hedef ülkelere odaklan."""
        leads = []
        
        if not brand:
            return leads
        
        # Target country specific searches
        target_queries = [
            # Turkey - en büyük market
            f'"{brand}" stenter Turkey Türkiye',
            f'"{brand}" ramöz tekstil Türkiye',
            f'"{brand}" finishing line Turkey Istanbul Bursa',
            # Brazil
            f'"{brand}" stenter Brazil Brasil',
            f'"{brand}" têxtil acabamento Brasil',
            # Egypt
            f'"{brand}" textile Egypt Alexandria Cairo',
            # Morocco
            f'"{brand}" textile Morocco Maroc Casablanca',
            # General OEM references
            f'"{brand}" installed customer textile finishing',
        ]
        
        for query in target_queries[:4]:  # Limit to 4 queries to save API
            results = self._brave_search(query)
            for r in results:
                companies = self._extract_companies_from_result(r, {"brand": brand})
                leads.extend(companies)
            
            time.sleep(0.5)
        
        return leads
    
    def _brave_search(self, query: str, count: int = 10) -> List[Dict]:
        """Brave Search API çağrısı."""
        if not self.api_key:
            return []
        
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": self.api_key
        }
        params = {
            "q": query,
            "count": count
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            self.stats["queries_made"] += 1
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("web", {}).get("results", [])
                self.stats["urls_found"] += len(results)
                return results
                
        except Exception as e:
            logger.warning(f"Brave search error: {e}")
        
        return []
    
    def _extract_companies_from_result(self, result: Dict, product: Dict) -> List[Dict]:
        """Brave sonucundan şirket bilgilerini çıkar."""
        companies = []
        
        title = result.get("title", "")
        description = result.get("description", "")
        url = result.get("url", "")
        
        full_text = f"{title} {description}"
        
        # Extract company names
        patterns = [
            r"([A-ZÇĞİÖŞÜ][a-zçğıöşü]+(?:\s+[A-ZÇĞİÖŞÜ][a-zçğıöşü]+)*)\s+(?:A\.?Ş\.?|Ltd\.?|San\.?)",
            r"([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)*)\s+(?:GmbH|AG|KG)",
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Ltd\.?|LLC|Inc\.?|Corp\.?)",
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Ltda\.?|S\.?A\.?)",
            r"([A-Z][A-Za-z\s&]+(?:Textile|Tekstil|Dyeing|Finishing|Fabrics?))",
        ]
        
        seen_names = set()
        for pattern in patterns:
            matches = re.findall(pattern, full_text)
            for match in matches:
                name = match.strip()
                if len(name) > 3 and name.lower() not in seen_names:
                    seen_names.add(name.lower())
                    
                    # Detect country from text
                    country = self._detect_country(full_text, url)
                    
                    # URL'den ülke tespit et (fallback)
                    if not country:
                        country = self._detect_country_from_url(url)
                    
                    # TÜM şirketleri kabul et (filtreleme CRM'de)
                    # Ancak OEM/sektör bilgisi varsa daha yüksek güven
                    has_textile_context = any(kw in full_text.lower() for kw in 
                        ["textile", "tekstil", "dyeing", "finishing", "stenter", "ramöz", "terbiye", "têxtil"])
                    
                    companies.append({
                        "company": name,
                        "country": country or "Unknown",
                        "context": full_text[:500],
                        "source_url": url,
                        "source_type": "precision_search",
                        "brand_mentioned": product.get("brand", ""),
                        "has_textile_context": has_textile_context,
                    })
        
        self.stats["companies_extracted"] += len(companies)
        return companies
    
    def _detect_country(self, text: str, url: str = "") -> str:
        """Metinden veya URL'den ülke tespit et."""
        text_lower = text.lower()
        
        country_indicators = {
            "Turkey": ["türkiye", "turkey", "istanbul", "bursa", "denizli", "gaziantep"],
            "Egypt": ["egypt", "mısır", "cairo", "alexandria"],
            "Brazil": ["brazil", "brasil", "são paulo", "rio"],
            "Morocco": ["morocco", "maroc", "casablanca", "rabat"],
            "Tunisia": ["tunisia", "tunisie", "tunis"],
            "Argentina": ["argentina", "buenos aires"],
            "Colombia": ["colombia", "bogotá", "medellín"],
            "Peru": ["peru", "perú", "lima"],
            "India": ["india", "mumbai", "delhi"],
            "Pakistan": ["pakistan", "karachi", "lahore"],
            "Bangladesh": ["bangladesh", "dhaka"],
        }
        
        for country, indicators in country_indicators.items():
            for indicator in indicators:
                if indicator in text_lower:
                    return country
        
        return ""
    
    def _detect_country_from_url(self, url: str) -> str:
        """URL'den ülke tespit et - sadece domain TLD'ye bak."""
        import re
        
        # Extract domain from URL
        domain_match = re.search(r'https?://(?:www\.)?([^/]+)', url)
        if not domain_match:
            return ""
        
        domain = domain_match.group(1).lower()
        
        # Get the actual TLD (last part after last dot)
        parts = domain.split('.')
        if len(parts) < 2:
            return ""
        
        # Check for country code TLDs at the end
        tld = parts[-1]
        
        # Second level for .com.xx patterns
        if len(parts) >= 3 and parts[-2] in ["com", "org", "net", "co"]:
            tld = parts[-1]
        
        tld_to_country = {
            "tr": "Turkey",
            "br": "Brazil",
            "eg": "Egypt",
            "ma": "Morocco",
            "tn": "Tunisia",
            "ar": "Argentina",
            "pe": "Peru",
            "in": "India",
            "pk": "Pakistan",
            "bd": "Bangladesh",
        }
        
        # Only match if actual TLD (not .com, .org, etc.)
        if tld in tld_to_country:
            return tld_to_country[tld]
        
        return ""
    
    def harvest(self) -> List[Dict]:
        """
        Ana harvest fonksiyonu - pipeline entegrasyonu için.
        
        Returns:
            List of lead dicts
        """
        logger.info("="*60)
        logger.info("🎯 PRECISION CUSTOMER FINDER - Starting harvest")
        logger.info("="*60)
        
        all_leads = []
        
        # 1. Product-based search
        logger.info("\n📦 Phase 1: Product-based search")
        product_leads = self.find_all_customers(max_products=8)
        all_leads.extend(product_leads)
        
        # 2. Multi-language search
        logger.info("\n🌍 Phase 2: Multi-language search")
        multilang_leads = self.find_customers_multilang()
        all_leads.extend(multilang_leads)
        
        # Dedupe
        seen = set()
        unique_leads = []
        for lead in all_leads:
            name = lead.get("company", "").lower()
            if name and name not in seen:
                seen.add(name)
                unique_leads.append(lead)
        
        logger.info(f"\n📊 STATS:")
        logger.info(f"  Queries made: {self.stats['queries_made']}")
        logger.info(f"  URLs found: {self.stats['urls_found']}")
        logger.info(f"  Companies extracted: {self.stats['companies_extracted']}")
        logger.info(f"  Unique leads: {len(unique_leads)}")
        
        return unique_leads


def harvest() -> List[Dict]:
    """Standalone harvest function for pipeline."""
    finder = PrecisionCustomerFinder()
    return finder.harvest()


if __name__ == "__main__":
    # Test run
    finder = PrecisionCustomerFinder()
    
    # Test single product
    if finder.products:
        product = finder.products[0]
        print(f"\nTesting with: {product.get('name')}")
        leads = finder.find_customers_for_product(product, max_queries=2)
        
        print(f"\nFound {len(leads)} leads:")
        for lead in leads[:5]:
            print(f"  - {lead.get('company')} ({lead.get('country')})")
