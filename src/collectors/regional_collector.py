#!/usr/bin/env python3
"""
Regional Collector - Güney Amerika ve Kuzey Afrika odaklı lead toplama
Comtrade verileri + yerel dizinler + fuar katılımcıları
"""

import os
import re
import logging
from typing import Dict, List, Optional
from pathlib import Path
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RegionalCollector:
    """Bölge bazlı lead toplama - Güney Amerika ve Kuzey Afrika öncelikli."""
    
    def __init__(self):
        self.base_path = Path(__file__).parent.parent.parent
        self.config_path = self.base_path / "config"
        self.targets = self._load_config("targets.yaml")
        self.products = self._load_config("products.yaml")
        
        self.brave_api_key = os.getenv("BRAVE_API_KEY", "BSAYTcCa5ZtcjOYZCEduotyNwmZVRXa")
        
        # HS codes for customer search
        self.hs_codes = ["845190", "848330", "848340"]
        
        # OEM brands
        self.oem_brands = ["Brückner", "Monforts", "Krantz", "Artos", "Santex"]
        
    def _load_config(self, filename: str) -> Dict:
        """Load YAML config."""
        path = self.config_path / filename
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        return {}
    
    def collect_south_america(self) -> List[Dict]:
        """
        Güney Amerika tekstil finishing şirketlerini topla.
        Öncelik: Brazil > Argentina > Colombia > Mexico > Peru
        """
        logger.info("=" * 60)
        logger.info("🌎 SOUTH AMERICA COLLECTOR")
        logger.info("=" * 60)
        
        leads = []
        
        region_config = self.targets.get("south_america", {})
        countries = region_config.get("countries", {})
        
        for country_key, country_data in countries.items():
            country_name = country_data.get("labels", [country_key])[0]
            priority = country_data.get("priority", 3)
            
            logger.info(f"\n🇧🇷 Collecting from {country_name} (priority: {priority})")
            
            # 1. Known manufacturers
            known = country_data.get("known_manufacturers", [])
            for manufacturer in known:
                leads.append({
                    "company": manufacturer,
                    "country": country_name,
                    "source_type": "known_manufacturer",
                    "priority": priority,
                    "region": "south_america"
                })
                logger.info(f"  ✓ Known: {manufacturer}")
            
            # 2. Search for OEM customers
            search_keywords = country_data.get("search_keywords", [])
            for keyword in search_keywords[:3]:
                found = self._brave_search_leads(keyword, country_name)
                leads.extend(found)
            
            # 3. OEM-specific search
            for oem in self.oem_brands[:2]:  # Focus on Brückner and Monforts
                oem_leads = self._search_oem_customers(oem, country_name)
                leads.extend(oem_leads)
        
        # Deduplicate
        unique_leads = self._dedupe_leads(leads)
        
        logger.info(f"\n✅ South America: {len(unique_leads)} unique leads")
        return unique_leads
    
    def collect_north_africa(self) -> List[Dict]:
        """
        Kuzey Afrika tekstil finishing şirketlerini topla.
        Öncelik: Egypt > Morocco > Tunisia > Algeria
        """
        logger.info("=" * 60)
        logger.info("🌍 NORTH AFRICA COLLECTOR")
        logger.info("=" * 60)
        
        leads = []
        
        region_config = self.targets.get("north_africa", {})
        countries = region_config.get("countries", {})
        
        for country_key, country_data in countries.items():
            country_name = country_data.get("labels", [country_key])[0]
            priority = country_data.get("priority", 3)
            
            logger.info(f"\n🇪🇬 Collecting from {country_name} (priority: {priority})")
            
            # 1. Known manufacturers
            known = country_data.get("known_manufacturers", [])
            for manufacturer in known:
                leads.append({
                    "company": manufacturer,
                    "country": country_name,
                    "source_type": "known_manufacturer",
                    "priority": priority,
                    "region": "north_africa"
                })
                logger.info(f"  ✓ Known: {manufacturer}")
            
            # 2. Search keywords
            search_keywords = country_data.get("search_keywords", [])
            for keyword in search_keywords[:3]:
                found = self._brave_search_leads(keyword, country_name)
                leads.extend(found)
            
            # 3. OEM-specific search
            for oem in self.oem_brands[:2]:
                oem_leads = self._search_oem_customers(oem, country_name)
                leads.extend(oem_leads)
        
        # Deduplicate
        unique_leads = self._dedupe_leads(leads)
        
        logger.info(f"\n✅ North Africa: {len(unique_leads)} unique leads")
        return unique_leads
    
    def collect_south_asia(self) -> List[Dict]:
        """
        Güney Asya tekstil finishing şirketlerini topla.
        Öncelik: Pakistan > India > Bangladesh
        """
        logger.info("=" * 60)
        logger.info("🌏 SOUTH ASIA COLLECTOR")
        logger.info("=" * 60)
        
        leads = []
        
        region_config = self.targets.get("south_asia", {})
        countries = region_config.get("countries", {})
        
        for country_key, country_data in countries.items():
            country_name = country_data.get("labels", [country_key])[0]
            priority = country_data.get("priority", 3)
            
            logger.info(f"\n🇵🇰 Collecting from {country_name} (priority: {priority})")
            
            # 1. Known manufacturers
            known = country_data.get("known_manufacturers", [])
            for manufacturer in known:
                leads.append({
                    "company": manufacturer,
                    "country": country_name,
                    "source_type": "known_manufacturer",
                    "priority": priority,
                    "region": "south_asia"
                })
                logger.info(f"  ✓ Known: {manufacturer}")
            
            # 2. Search keywords
            search_keywords = country_data.get("search_keywords", [])
            for keyword in search_keywords[:2]:
                found = self._brave_search_leads(keyword, country_name)
                leads.extend(found)
        
        # Deduplicate
        unique_leads = self._dedupe_leads(leads)
        
        logger.info(f"\n✅ South Asia: {len(unique_leads)} unique leads")
        return unique_leads
    
    def _brave_search_leads(self, query: str, country: str, count: int = 20) -> List[Dict]:
        """Brave API ile lead ara."""
        import requests
        
        leads = []
        
        try:
            headers = {
                "Accept": "application/json",
                "X-Subscription-Token": self.brave_api_key
            }
            
            # Add textile finishing context
            enhanced_query = f"{query} textile dyeing finishing stenter"
            
            response = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": enhanced_query, "count": count},
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("web", {}).get("results", [])
                
                for result in results:
                    company = self._extract_company_name(
                        result.get("title", ""),
                        result.get("description", ""),
                        result.get("url", "")
                    )
                    
                    if company and self._is_valid_company(company):
                        leads.append({
                            "company": company,
                            "country": country,
                            "context": result.get("description", "")[:500],
                            "source_url": result.get("url", ""),
                            "source_type": "brave_search",
                            "search_query": query
                        })
                        
        except Exception as e:
            logger.error(f"Brave search error: {e}")
        
        return leads
    
    def _search_oem_customers(self, oem: str, country: str) -> List[Dict]:
        """Belirli OEM için müşteri ara."""
        import requests
        
        leads = []
        
        queries = [
            f"{oem} stenter {country}",
            f"{oem} customer {country} textile",
            f'"{oem}" finishing {country}',
            f"{oem} installation {country}"
        ]
        
        for query in queries[:2]:
            try:
                headers = {
                    "Accept": "application/json",
                    "X-Subscription-Token": self.brave_api_key
                }
                
                response = requests.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    params={"q": query, "count": 10},
                    headers=headers,
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    results = data.get("web", {}).get("results", [])
                    
                    for result in results:
                        title = result.get("title", "")
                        desc = result.get("description", "")
                        url = result.get("url", "")
                        
                        # Look for customer mentions
                        if oem.lower() in (title + desc).lower():
                            companies = self._extract_customer_from_context(title + " " + desc, oem)
                            
                            for company in companies:
                                if self._is_valid_company(company):
                                    leads.append({
                                        "company": company,
                                        "country": country,
                                        "context": desc[:500],
                                        "source_url": url,
                                        "source_type": "oem_customer",
                                        "oem_reference": oem,
                                        "brand_mentioned": oem
                                    })
                                    logger.info(f"    🎯 OEM Customer: {company} ({oem})")
                                    
            except Exception as e:
                logger.error(f"OEM search error: {e}")
        
        return leads
    
    def _extract_company_name(self, title: str, desc: str, url: str) -> Optional[str]:
        """Şirket adını çıkar."""
        text = title + " " + desc
        
        # Common patterns
        patterns = [
            r"([A-Z][a-zğüşıöç]+\s+(?:Têxtil|Textile|Textiles|Dyeing|Finishing|Mills|Manufacturing|S\.A\.|S\.A\.E\.|SARL|Ltda))",
            r"([A-Z][A-Za-zğüşıöç]+(?:\s+[A-Z][A-Za-zğüşıöç]+)*\s+(?:Group|Corporation|Industries|Limited|Ltd))",
            r"((?:Grupo|Tecelagem|Tinturaria|Tintorería|Acabados)\s+[A-Z][A-Za-zğüşıöç]+)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        
        # Fallback: first significant capitalized phrase
        words = title.split()
        for i, word in enumerate(words):
            if word[0].isupper() and len(word) > 3:
                # Take up to 3 consecutive capitalized words
                company_parts = [word]
                for j in range(i+1, min(i+3, len(words))):
                    if words[j][0].isupper():
                        company_parts.append(words[j])
                    else:
                        break
                return " ".join(company_parts)
        
        return None
    
    def _extract_customer_from_context(self, text: str, oem: str) -> List[str]:
        """OEM referansından müşteri adını çıkar."""
        customers = []
        
        # Patterns like "installed at XYZ Textile"
        patterns = [
            rf"(?:installed|delivered|ordered|commissioned)\s+(?:at|by|for|to)\s+([A-Z][A-Za-z\s]+(?:Textile|Mills|Group|Manufacturing))",
            rf"([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)\s+(?:ordered|installed|uses)\s+{oem}",
            rf"(?:customer|client)\s+([A-Z][A-Za-z\s]+)",
            rf"([A-Z][A-Za-z]+\s+(?:Têxtil|Textile|Textiles|Mills))\s+.*?{oem}",
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            customers.extend(matches)
        
        # Clean and dedupe
        cleaned = []
        for c in customers:
            c = c.strip()
            if len(c) > 3 and c not in cleaned:
                # Remove OEM name if accidentally captured
                if oem.lower() not in c.lower():
                    cleaned.append(c)
        
        return cleaned[:3]  # Max 3 per result
    
    def _is_valid_company(self, name: str) -> bool:
        """Check if company name is valid."""
        if not name or len(name) < 3:
            return False
        
        # Exclude common non-company terms
        excluded = [
            "textile", "finishing", "dyeing", "stenter", "machinery",
            "equipment", "parts", "technology", "solutions", "services",
            "association", "federation", "ministry", "government"
        ]
        
        name_lower = name.lower()
        if any(ex == name_lower for ex in excluded):
            return False
        
        return True
    
    def _dedupe_leads(self, leads: List[Dict]) -> List[Dict]:
        """Remove duplicate leads."""
        seen = set()
        unique = []
        
        for lead in leads:
            company = lead.get("company", "").lower().strip()
            if company and company not in seen:
                seen.add(company)
                unique.append(lead)
        
        return unique
    
    def collect_all_priority_regions(self) -> List[Dict]:
        """
        Tüm öncelikli bölgelerden lead topla.
        Sıra: South America > North Africa > South Asia
        """
        logger.info("\n" + "=" * 70)
        logger.info("🌍 COLLECTING FROM ALL PRIORITY REGIONS")
        logger.info("=" * 70)
        
        all_leads = []
        
        # Priority 1: South America
        south_america = self.collect_south_america()
        all_leads.extend(south_america)
        
        # Priority 1: North Africa
        north_africa = self.collect_north_africa()
        all_leads.extend(north_africa)
        
        # Priority 2: South Asia
        south_asia = self.collect_south_asia()
        all_leads.extend(south_asia)
        
        # Final dedupe
        final_leads = self._dedupe_leads(all_leads)
        
        logger.info("\n" + "=" * 70)
        logger.info("📊 COLLECTION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"  South America: {len(south_america)} leads")
        logger.info(f"  North Africa: {len(north_africa)} leads")
        logger.info(f"  South Asia: {len(south_asia)} leads")
        logger.info(f"  Total unique: {len(final_leads)} leads")
        
        return final_leads


if __name__ == "__main__":
    collector = RegionalCollector()
    leads = collector.collect_all_priority_regions()
    
    print(f"\n📋 Sample leads:")
    for lead in leads[:20]:
        print(f"  • {lead['company']} ({lead['country']}) - {lead.get('source_type', 'unknown')}")
