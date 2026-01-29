#!/usr/bin/env python3
"""
AutoDiscover Module - Yeni fuar ve kaynak keşfi + DOM tersine mühendislik
Comtrade API entegrasyonu ile gerçek ithalatçıları bulur
"""

import os
import re
import json
import yaml
import hashlib
import logging
from typing import Dict, List, Optional, Set, Any
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AutoDiscover:
    """Otomatik kaynak keşfi ve DOM tersine mühendislik."""
    
    def __init__(self, config_path: str = None):
        self.base_path = Path(__file__).parent.parent.parent
        self.config_path = config_path or self.base_path / "config"
        self.data_path = self.base_path / "data"
        self.cache_path = self.data_path / "raw" / "autodiscover"
        self.cache_path.mkdir(parents=True, exist_ok=True)
        
        # Load configs
        self.targets = self._load_yaml("targets.yaml")
        self.products = self._load_yaml("products.yaml")
        
        # Discovered sources storage
        self.discovered_sources_path = self.data_path / "staging" / "discovered_sources.yaml"
        self.discovered_sources = self._load_discovered_sources()
        
        # HTTP client
        self.http_client = None
        self.brave_api_key = os.getenv("BRAVE_API_KEY", "BSAYTcCa5ZtcjOYZCEduotyNwmZVRXa")
        
    def _load_yaml(self, filename: str) -> Dict:
        """Load YAML config file."""
        path = self.config_path / filename
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        return {}
    
    def _load_discovered_sources(self) -> Dict:
        """Load previously discovered sources."""
        if self.discovered_sources_path.exists():
            with open(self.discovered_sources_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {"fairs": [], "directories": [], "scraped_urls": []}
        return {"fairs": [], "directories": [], "scraped_urls": []}
    
    def _save_discovered_sources(self):
        """Save discovered sources."""
        with open(self.discovered_sources_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.discovered_sources, f, default_flow_style=False, allow_unicode=True)
    
    def _get_http_client(self):
        """Get or create HTTP client."""
        if self.http_client is None:
            import sys
            sys.path.insert(0, str(self.base_path / "src"))
            from utils.http_client import HTTPClient
            self.http_client = HTTPClient()
        return self.http_client
    
    def discover_new_fairs(self, regions: List[str] = None) -> List[Dict]:
        """
        Yeni tekstil fuarlarını keşfet.
        
        Args:
            regions: Aranacak bölgeler (None = tümü)
            
        Returns:
            Keşfedilen fuarların listesi
        """
        logger.info("🔍 Discovering new textile trade fairs...")
        
        discovered = []
        
        # Bölge bazlı arama terimleri
        region_terms = {
            "south_america": [
                "textile fair South America 2025 2026",
                "Brazil textile exhibition",
                "Colombia textile trade show",
                "Argentina textile fair",
                "Peru textile expo",
                "Febratex exhibitors",
                "Colombiatex exhibitors list",
                "FIMEC Brazil exhibitors"
            ],
            "north_africa": [
                "Egypt textile fair 2025 2026",
                "Morocco textile exhibition",
                "Tunisia textile trade show",
                "Cairo Fashion Tex exhibitors",
                "Maroc Sourcing exhibitors",
                "Africa textile machinery expo"
            ],
            "south_asia": [
                "Pakistan textile fair IGATEX",
                "India ITME exhibitors",
                "Bangladesh textile expo",
                "Karachi textile exhibition",
                "GTTES India exhibitors"
            ],
            "turkey": [
                "ITM Istanbul exhibitors",
                "Turkey textile machinery fair"
            ]
        }
        
        search_terms = []
        target_regions = regions or list(region_terms.keys())
        
        for region in target_regions:
            if region in region_terms:
                search_terms.extend(region_terms[region])
        
        # Brave API ile arama
        for term in search_terms[:10]:  # Limit searches
            results = self._brave_search(term)
            
            for result in results:
                url = result.get("url", "")
                title = result.get("title", "")
                
                # Fuar/exhibitor patternleri kontrol et
                if self._is_fair_url(url, title):
                    fair_info = {
                        "name": self._extract_fair_name(title, url),
                        "url": url,
                        "title": title,
                        "discovered_via": term,
                        "discovered_at": datetime.now().isoformat(),
                        "region": self._detect_region(url, title)
                    }
                    
                    # Duplicate check
                    if not any(f["url"] == url for f in discovered):
                        discovered.append(fair_info)
                        logger.info(f"  🎪 Found: {fair_info['name']} ({fair_info['region']})")
        
        # Save discoveries
        for fair in discovered:
            if not any(f.get("url") == fair["url"] for f in self.discovered_sources.get("fairs", [])):
                self.discovered_sources.setdefault("fairs", []).append(fair)
        
        self._save_discovered_sources()
        
        logger.info(f"✅ Discovered {len(discovered)} new fairs")
        return discovered
    
    def _brave_search(self, query: str, count: int = 10) -> List[Dict]:
        """Execute Brave API search."""
        import requests
        
        try:
            headers = {
                "Accept": "application/json",
                "X-Subscription-Token": self.brave_api_key
            }
            
            response = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": query, "count": count},
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("web", {}).get("results", [])
        except Exception as e:
            logger.error(f"Brave search error: {e}")
        
        return []
    
    def _is_fair_url(self, url: str, title: str) -> bool:
        """Check if URL is a trade fair page."""
        fair_patterns = [
            r"exhibitor", r"aussteller", r"exposant",
            r"trade.?fair", r"messe", r"salon",
            r"exhibition", r"expo", r"fair",
            r"participant", r"company.?list"
        ]
        
        text = (url + " " + title).lower()
        return any(re.search(p, text) for p in fair_patterns)
    
    def _extract_fair_name(self, title: str, url: str) -> str:
        """Extract fair name from title/URL."""
        # Common patterns
        fair_names = [
            "ITMA", "ITM", "Techtextil", "Texprocess", "Heimtextil",
            "IGATEX", "Colombiatex", "Febratex", "FIMEC", "GTTES",
            "Cairo Fashion", "Maroc Sourcing", "India ITME"
        ]
        
        for name in fair_names:
            if name.lower() in title.lower() or name.lower() in url.lower():
                return name
        
        # Extract from URL domain
        domain = urlparse(url).netloc.replace("www.", "")
        return domain.split(".")[0].title()
    
    def _detect_region(self, url: str, title: str) -> str:
        """Detect region from URL/title."""
        text = (url + " " + title).lower()
        
        region_keywords = {
            "south_america": ["brazil", "brasil", "argentina", "colombia", "peru", "mexico"],
            "north_africa": ["egypt", "morocco", "tunisia", "algeria", "cairo", "maroc"],
            "south_asia": ["pakistan", "india", "bangladesh", "karachi", "delhi"],
            "turkey": ["turkey", "istanbul", "itm", "turkiye"],
            "europe": ["frankfurt", "germany", "milan", "paris", "europe"]
        }
        
        for region, keywords in region_keywords.items():
            if any(kw in text for kw in keywords):
                return region
        
        return "unknown"
    
    def auto_scrape_exhibitors(self, url: str, fair_name: str = None) -> List[Dict]:
        """
        Otomatik DOM tersine mühendislik ile katılımcı listesi çek.
        
        Args:
            url: Fuar katılımcı sayfası URL'i
            fair_name: Fuar adı
            
        Returns:
            Çıkarılan katılımcı listesi
        """
        logger.info(f"🔧 Auto-scraping exhibitors from: {url}")
        
        try:
            http = self._get_http_client()
            html = http.fetch_url(url)
            
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # DOM pattern detection
            exhibitors = []
            
            # Try multiple patterns
            patterns = self._get_dom_patterns()
            
            for pattern in patterns:
                found = self._extract_with_pattern(soup, pattern)
                if found:
                    exhibitors.extend(found)
                    logger.info(f"  ✓ Pattern matched: {len(found)} exhibitors")
                    break
            
            # If patterns fail, try intelligent extraction
            if not exhibitors:
                exhibitors = self._intelligent_extract(soup)
            
            # Add metadata
            for ex in exhibitors:
                ex["source_url"] = url
                ex["source_name"] = fair_name or self._extract_fair_name("", url)
                ex["source_type"] = "fair"
                ex["scraped_at"] = datetime.now().isoformat()
            
            # Save to cache
            self._cache_scrape_result(url, exhibitors)
            
            logger.info(f"✅ Extracted {len(exhibitors)} exhibitors from {url}")
            return exhibitors
            
        except Exception as e:
            logger.error(f"Auto-scrape error: {e}")
            return []
    
    def _get_dom_patterns(self) -> List[Dict]:
        """Get DOM extraction patterns from config."""
        patterns = self.targets.get("dom_patterns", {})
        
        # Exhibitor patterns
        exhibitor_patterns = patterns.get("exhibitor_patterns", [])
        
        # Add default patterns if empty
        if not exhibitor_patterns:
            exhibitor_patterns = [
                {
                    "selector": ".exhibitor-card, .exhibitor-item, .company-card, .member-item",
                    "fields": {
                        "company": ".company-name, .exhibitor-name, .name, h3, h4, .title",
                        "country": ".country, .location, .address",
                        "website": "a[href^='http']",
                        "booth": ".booth, .stand"
                    }
                },
                {
                    "selector": "table tr, .table-row",
                    "fields": {
                        "company": "td:first-child, .col-name",
                        "country": "td:nth-child(2), .col-country",
                        "website": "a[href]"
                    }
                },
                {
                    "selector": ".list-item, .directory-item, li.company",
                    "fields": {
                        "company": ".name, .title, a:first-child",
                        "country": ".meta, .location"
                    }
                }
            ]
        
        return exhibitor_patterns
    
    def _extract_with_pattern(self, soup: BeautifulSoup, pattern: Dict) -> List[Dict]:
        """Extract data using a specific pattern."""
        results = []
        
        selector = pattern.get("selector", "")
        fields = pattern.get("fields", {})
        
        items = soup.select(selector)
        
        for item in items:
            data = {}
            
            for field_name, field_selector in fields.items():
                # Try multiple selectors
                selectors = [s.strip() for s in field_selector.split(",")]
                
                for sel in selectors:
                    try:
                        el = item.select_one(sel)
                        if el:
                            if field_name == "website":
                                data[field_name] = el.get("href", "")
                            else:
                                data[field_name] = el.get_text(strip=True)
                            break
                    except:
                        continue
            
            # Only add if we found a company name
            if data.get("company"):
                results.append(data)
        
        return results
    
    def _intelligent_extract(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Intelligent extraction when patterns fail.
        Tries to detect repeating structures.
        """
        results = []
        
        # Find all elements with similar class patterns
        # Look for repeated structures
        
        # Strategy 1: Find lists
        for ul in soup.find_all(['ul', 'ol']):
            items = ul.find_all('li', recursive=False)
            if len(items) > 5:  # Likely a list of companies
                for li in items:
                    text = li.get_text(strip=True)
                    if len(text) > 3 and len(text) < 200:
                        # Try to find link
                        link = li.find('a')
                        results.append({
                            "company": text.split('\n')[0].strip(),
                            "website": link.get('href', '') if link else ''
                        })
        
        # Strategy 2: Find repeated div patterns
        all_divs = soup.find_all('div', class_=True)
        class_counts = {}
        
        for div in all_divs:
            classes = ' '.join(sorted(div.get('class', [])))
            class_counts[classes] = class_counts.get(classes, 0) + 1
        
        # Find most repeated pattern (likely company cards)
        repeated_classes = [c for c, count in class_counts.items() if count > 5]
        
        for class_pattern in repeated_classes[:3]:
            divs = soup.find_all('div', class_=class_pattern.split())
            for div in divs:
                # Find company name (usually first heading or strong text)
                name_el = div.find(['h1', 'h2', 'h3', 'h4', 'h5', 'strong', 'b'])
                if name_el:
                    link = div.find('a', href=True)
                    results.append({
                        "company": name_el.get_text(strip=True),
                        "website": link.get('href', '') if link else ''
                    })
        
        return results
    
    def _cache_scrape_result(self, url: str, data: List[Dict]):
        """Cache scrape results."""
        url_hash = hashlib.md5(url.encode()).hexdigest()[:16]
        cache_file = self.cache_path / f"exhibitors_{url_hash}.json"
        
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump({
                "url": url,
                "scraped_at": datetime.now().isoformat(),
                "count": len(data),
                "data": data
            }, f, indent=2, ensure_ascii=False)
    
    def get_comtrade_importers(self, hs_code: str = "845190", year: int = 2023) -> List[Dict]:
        """
        Comtrade API ile belirli HS kodu için ithalatçı ülkeleri bul.
        
        Args:
            hs_code: HS kodu
            year: Yıl
            
        Returns:
            İthalatçı ülkeler listesi (en büyükten küçüğe)
        """
        logger.info(f"📊 Fetching Comtrade importers for HS {hs_code}...")
        
        # Check cache first
        cache_file = self.cache_path / f"comtrade_{hs_code}_{year}.json"
        
        if cache_file.exists():
            with open(cache_file, 'r') as f:
                cached = json.load(f)
                if cached.get("data"):
                    logger.info(f"  Using cached Comtrade data")
                    return cached["data"]
        
        # Fetch from API
        import requests
        
        try:
            # UN Comtrade API endpoint
            url = "https://comtradeapi.un.org/data/v1/get/C/A"
            
            params = {
                "cmdCode": hs_code,
                "flowCode": "M",  # Imports
                "period": str(year),
                "reporterCode": "all",  # All countries
                "partnerCode": "0"  # World
            }
            
            # Note: Comtrade requires registration for full access
            # Using public endpoint which has limits
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Parse and sort by import value
                importers = []
                for record in data.get("data", []):
                    importers.append({
                        "country_code": record.get("reporterCode"),
                        "country": record.get("reporterDesc"),
                        "import_value_usd": record.get("primaryValue", 0),
                        "year": year,
                        "hs_code": hs_code
                    })
                
                # Sort by import value
                importers.sort(key=lambda x: x["import_value_usd"], reverse=True)
                
                # Cache results
                with open(cache_file, 'w') as f:
                    json.dump({"data": importers, "fetched_at": datetime.now().isoformat()}, f)
                
                return importers
                
        except Exception as e:
            logger.error(f"Comtrade API error: {e}")
        
        # Return from local priority data if API fails
        return self._get_priority_countries()
    
    def _get_priority_countries(self) -> List[Dict]:
        """Get priority countries from config when Comtrade fails."""
        priority_data = []
        
        # Parse targets.yaml for priority countries
        for region_key in ["south_america", "north_africa", "south_asia", "turkey"]:
            region = self.targets.get(region_key, {})
            region_priority = region.get("priority", 5)
            
            for country_key, country_data in region.get("countries", {}).items():
                priority_data.append({
                    "country_code": country_data.get("comtrade_reporter"),
                    "country": country_data.get("labels", [country_key])[0],
                    "region": region_key,
                    "priority": region_priority,
                    "notes": country_data.get("notes", "")
                })
        
        # Sort by priority
        priority_data.sort(key=lambda x: x["priority"])
        
        return priority_data
    
    def discover_industry_directories(self) -> List[Dict]:
        """Discover new industry directories for target regions."""
        logger.info("🔍 Discovering industry directories...")
        
        discovered = []
        
        search_terms = [
            "Brazil textile manufacturers directory",
            "Argentina textile companies list",
            "Egypt textile factory directory",
            "Morocco textile suppliers",
            "Pakistan textile mills directory APTMA",
            "India textile manufacturers CITI",
            "Colombia textile companies",
            "Peru textile exporters",
            "Tunisia textile manufacturers"
        ]
        
        for term in search_terms:
            results = self._brave_search(term, count=5)
            
            for result in results:
                url = result.get("url", "")
                title = result.get("title", "")
                
                if self._is_directory_url(url, title):
                    dir_info = {
                        "name": title[:100],
                        "url": url,
                        "type": "industry_directory",
                        "discovered_via": term,
                        "discovered_at": datetime.now().isoformat()
                    }
                    
                    if not any(d["url"] == url for d in discovered):
                        discovered.append(dir_info)
                        logger.info(f"  📂 Found: {dir_info['name'][:50]}")
        
        # Save discoveries
        for dir_info in discovered:
            if not any(d.get("url") == dir_info["url"] for d in self.discovered_sources.get("directories", [])):
                self.discovered_sources.setdefault("directories", []).append(dir_info)
        
        self._save_discovered_sources()
        
        logger.info(f"✅ Discovered {len(discovered)} directories")
        return discovered
    
    def _is_directory_url(self, url: str, title: str) -> bool:
        """Check if URL is a business directory."""
        dir_patterns = [
            r"director", r"compan", r"manufacturer", r"supplier",
            r"member", r"list", r"factory", r"mill", r"exporter"
        ]
        
        text = (url + " " + title).lower()
        return any(re.search(p, text) for p in dir_patterns)
    
    def run_full_discovery(self, focus_regions: List[str] = None) -> Dict:
        """
        Tam otomatik keşif çalıştır.
        
        Args:
            focus_regions: Odaklanılacak bölgeler
            
        Returns:
            Keşif sonuçları
        """
        logger.info("=" * 60)
        logger.info("🚀 STARTING FULL AUTO-DISCOVERY")
        logger.info("=" * 60)
        
        focus = focus_regions or ["south_america", "north_africa", "south_asia"]
        
        results = {
            "fairs": [],
            "directories": [],
            "priority_countries": [],
            "exhibitors_scraped": 0
        }
        
        # 1. Discover new fairs
        logger.info("\n📌 Phase 1: Discovering trade fairs...")
        results["fairs"] = self.discover_new_fairs(focus)
        
        # 2. Auto-scrape discovered fairs
        logger.info("\n📌 Phase 2: Auto-scraping exhibitor lists...")
        for fair in results["fairs"][:5]:  # Limit to top 5
            exhibitors = self.auto_scrape_exhibitors(
                fair["url"], 
                fair.get("name", "Unknown Fair")
            )
            results["exhibitors_scraped"] += len(exhibitors)
        
        # 3. Discover industry directories
        logger.info("\n📌 Phase 3: Discovering industry directories...")
        results["directories"] = self.discover_industry_directories()
        
        # 4. Get priority countries from Comtrade
        logger.info("\n📌 Phase 4: Getting import priority data...")
        results["priority_countries"] = self._get_priority_countries()
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ AUTO-DISCOVERY COMPLETE")
        logger.info(f"   Fairs discovered: {len(results['fairs'])}")
        logger.info(f"   Directories found: {len(results['directories'])}")
        logger.info(f"   Exhibitors scraped: {results['exhibitors_scraped']}")
        logger.info("=" * 60)
        
        return results


class ComtradeIntelligence:
    """Comtrade API ile akıllı ithalatçı analizi."""
    
    def __init__(self):
        self.base_path = Path(__file__).parent.parent.parent
        self.cache_path = self.base_path / "data" / "processed"
        self.cache_path.mkdir(parents=True, exist_ok=True)
        
    def get_top_importers_for_products(self, hs_codes: List[str] = None) -> Dict[str, List]:
        """
        Ürün HS kodlarına göre en büyük ithalatçıları bul.
        
        Args:
            hs_codes: HS kod listesi
            
        Returns:
            Her HS kodu için ithalatçı listesi
        """
        if hs_codes is None:
            hs_codes = ["845190", "848330", "848340"]
        
        results = {}
        
        for code in hs_codes:
            cache_file = self.cache_path / f"country_priority_{code}.csv"
            
            if cache_file.exists():
                # Load from cache
                import csv
                with open(cache_file, 'r') as f:
                    reader = csv.DictReader(f)
                    results[code] = list(reader)
            else:
                # Use priority data
                results[code] = self._get_manual_priority()
        
        return results
    
    def _get_manual_priority(self) -> List[Dict]:
        """Manual priority when Comtrade unavailable."""
        return [
            {"country": "Turkey", "priority": 100, "notes": "Largest stenter market"},
            {"country": "Brazil", "priority": 90, "notes": "Denim hub"},
            {"country": "Egypt", "priority": 85, "notes": "Growing fast"},
            {"country": "Argentina", "priority": 80, "notes": "AustralTex"},
            {"country": "Pakistan", "priority": 75, "notes": "Large textile"},
            {"country": "India", "priority": 70, "notes": "Massive volume"},
            {"country": "Morocco", "priority": 65, "notes": "EU proximity"},
            {"country": "Colombia", "priority": 60, "notes": "Growing"},
            {"country": "Tunisia", "priority": 55, "notes": "Denim"},
            {"country": "Bangladesh", "priority": 50, "notes": "RMG focus"},
            {"country": "Mexico", "priority": 45, "notes": "Zentrix"},
            {"country": "Peru", "priority": 40, "notes": "Cotton quality"}
        ]
    
    def score_country_potential(self, country: str, hs_code: str = "845190") -> int:
        """
        Ülke potansiyelini skorla.
        
        Args:
            country: Ülke adı
            hs_code: HS kodu
            
        Returns:
            Potansiyel skoru (0-100)
        """
        priority_data = self._get_manual_priority()
        
        for data in priority_data:
            if data["country"].lower() == country.lower():
                return data["priority"]
        
        return 20  # Default score for unknown countries


if __name__ == "__main__":
    # Test AutoDiscover
    discoverer = AutoDiscover()
    
    # Run discovery focusing on priority regions
    results = discoverer.run_full_discovery(
        focus_regions=["south_america", "north_africa"]
    )
    
    print(f"\n📊 Discovery Results:")
    print(f"  Fairs: {len(results['fairs'])}")
    print(f"  Directories: {len(results['directories'])}")
    print(f"  Exhibitors: {results['exhibitors_scraped']}")
