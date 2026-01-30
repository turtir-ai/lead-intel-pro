#!/usr/bin/env python3
"""
AutoScraper - Otomatik Tersine Mühendislik ile Veri Kazıma

Brave API'den gelen URL'leri analiz eder ve:
1. Sayfa yapısını analiz eder (DOM elemanları)
2. API endpoint'lerini yakalar (XHR/Fetch)
3. Şirket bilgilerini çıkarır (firma adı, ülke, iletişim)
4. Pagination pattern'lerini tespit eder
5. Otomatik scraping adapter'ı oluşturur

Bu modül LLM kullanmaz - pure Python pattern matching.
"""

import asyncio
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urljoin, urlparse, parse_qs
from dataclasses import dataclass, field, asdict
from datetime import datetime

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, Page, Response
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from src.utils.logger import get_logger
from src.utils.http_client import HttpClient
from src.utils.evidence import record_evidence

logger = get_logger(__name__)

# Evidence path for autodiscover
EVIDENCE_PATH = Path(__file__).parent.parent.parent / "outputs" / "evidence" / "autodiscover_log.csv"


@dataclass
class ApiEndpoint:
    """Discovered API endpoint."""
    url: str
    method: str = "GET"
    content_type: str = ""
    params: Dict = field(default_factory=dict)
    response_sample: str = ""
    data_path: str = ""  # JSON path to data array
    pagination_type: str = ""  # offset, page, cursor, scroll
    pagination_param: str = ""


@dataclass
class CompanyExtraction:
    """Extracted company data."""
    name: str
    country: str = ""
    city: str = ""
    website: str = ""
    email: str = ""
    phone: str = ""
    context: str = ""
    source_url: str = ""
    extraction_method: str = ""  # dom, api, table, list


class AutoScraper:
    """
    Otomatik tersine mühendislik ve veri kazıma motoru.
    
    Strateji:
    1. URL'yi ziyaret et (Playwright ile network capture)
    2. XHR/Fetch isteklerini yakala
    3. JSON API varsa → API scraping
    4. Yoksa → DOM scraping (table, list, cards)
    5. Pattern'leri öğren ve adapter oluştur
    """
    
    # Şirket adı için regex pattern'ler
    COMPANY_PATTERNS = [
        # Türk şirket formları
        r"([A-ZÇĞİÖŞÜ][a-zçğıöşü]+(?:\s+[A-ZÇĞİÖŞÜ][a-zçğıöşü]+)*)\s+(?:A\.?Ş\.?|Ltd\.?|San\.?\s*(?:ve\s*)?Tic\.?)",
        # Alman şirket formları  
        r"([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)*)\s+(?:GmbH|AG|KG|e\.?K\.?)",
        # İngilizce şirket formları
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Ltd\.?|LLC|Inc\.?|Corp\.?|Co\.?)",
        # Brezilya şirket formları
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Ltda\.?|S\.?A\.?|Cia\.?)",
        # Genel pattern
        r"([A-Z][A-Za-z\s&]+(?:Textile|Tekstil|Dyeing|Finishing|Fabrics?|Mills?|Industries?))",
    ]
    
    # E-posta pattern
    EMAIL_PATTERN = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    
    # Telefon pattern (international)
    PHONE_PATTERN = r"[\+]?[(]?[0-9]{1,4}[)]?[-\s\.]?[0-9]{1,4}[-\s\.]?[0-9]{1,4}[-\s\.]?[0-9]{1,9}"
    
    # Website pattern
    WEBSITE_PATTERN = r"(?:https?://)?(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z]{2,})(?:/[^\s]*)?"
    
    # Country indicators
    COUNTRY_INDICATORS = {
        "Turkey": ["Türkiye", "Turkey", "Istanbul", "İstanbul", "Bursa", "Denizli", "Gaziantep", ".tr"],
        "Egypt": ["Egypt", "Mısır", "Cairo", "Alexandria", ".eg"],
        "Brazil": ["Brazil", "Brasil", "São Paulo", "Rio", ".br"],
        "Morocco": ["Morocco", "Maroc", "Casablanca", "Rabat", ".ma"],
        "Tunisia": ["Tunisia", "Tunisie", "Tunis", ".tn"],
        "Argentina": ["Argentina", "Buenos Aires", ".ar"],
        "Colombia": ["Colombia", "Bogotá", "Medellín", ".co"],
        "Peru": ["Peru", "Perú", "Lima", ".pe"],
        "Germany": ["Germany", "Deutschland", ".de"],
        "India": ["India", "Mumbai", "Delhi", ".in"],
        "Pakistan": ["Pakistan", "Karachi", "Lahore", ".pk"],
        "Bangladesh": ["Bangladesh", "Dhaka", ".bd"],
    }

    def __init__(self):
        self.http_client = HttpClient()
        self.cache_dir = Path("data/cache/autoscraper")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Network capture storage
        self.captured_requests: List[Dict] = []
        self.captured_responses: List[Dict] = []
    
    async def analyze_url(self, url: str, use_playwright: bool = True) -> Dict:
        """
        URL'yi analiz et ve scraping stratejisi belirle.
        
        Returns:
            {
                "url": str,
                "strategy": "api" | "dom" | "hybrid",
                "api_endpoints": [...],
                "dom_selectors": {...},
                "companies_found": [...],
                "pagination": {...}
            }
        """
        result = {
            "url": url,
            "strategy": "dom",
            "api_endpoints": [],
            "dom_selectors": {},
            "companies_found": [],
            "pagination": None,
            "analyzed_at": datetime.now().isoformat()
        }
        
        if use_playwright and PLAYWRIGHT_AVAILABLE:
            # Playwright ile network capture + DOM analizi
            result = await self._analyze_with_playwright(url, result)
        else:
            # Basit HTTP request ile DOM analizi
            result = self._analyze_with_requests(url, result)
        
        # Strateji belirleme
        if result["api_endpoints"]:
            result["strategy"] = "api"
        elif result["dom_selectors"]:
            result["strategy"] = "dom"
        
        return result
    
    async def _analyze_with_playwright(self, url: str, result: Dict) -> Dict:
        """Playwright ile network capture ve DOM analizi."""
        self.captured_requests = []
        self.captured_responses = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()
            
            # Network request capture
            async def handle_request(request):
                if request.resource_type in ["xhr", "fetch"]:
                    self.captured_requests.append({
                        "url": request.url,
                        "method": request.method,
                        "headers": dict(request.headers),
                        "post_data": request.post_data
                    })
            
            async def handle_response(response):
                content_type = response.headers.get("content-type", "")
                if "json" in content_type or "xhr" in response.request.resource_type:
                    try:
                        body = await response.text()
                        self.captured_responses.append({
                            "url": response.url,
                            "status": response.status,
                            "content_type": content_type,
                            "body": body[:5000]  # Limit size
                        })
                    except:
                        pass
            
            page.on("request", handle_request)
            page.on("response", handle_response)
            
            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(2)  # Extra wait for lazy-loaded content
                
                # Scroll to trigger lazy loading
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                await asyncio.sleep(1)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)
                
                # Get page content
                html = await page.content()
                
            except Exception as e:
                logger.warning(f"Playwright error for {url}: {e}")
                html = ""
            
            await browser.close()
        
        # Analyze captured API calls
        result["api_endpoints"] = self._analyze_api_calls()
        
        # Analyze DOM
        if html:
            soup = BeautifulSoup(html, "html.parser")
            result["dom_selectors"] = self._analyze_dom_structure(soup)
            result["companies_found"] = self._extract_companies_from_dom(soup, url)
        
        return result
    
    def _analyze_with_requests(self, url: str, result: Dict) -> Dict:
        """Simple HTTP request ile DOM analizi."""
        try:
            html = self.http_client.get(url)
            if html:
                soup = BeautifulSoup(html, "html.parser")
                result["dom_selectors"] = self._analyze_dom_structure(soup)
                result["companies_found"] = self._extract_companies_from_dom(soup, url)
        except Exception as e:
            logger.warning(f"Request error for {url}: {e}")
        
        return result
    
    def _analyze_api_calls(self) -> List[Dict]:
        """Capture edilen API çağrılarını analiz et."""
        endpoints = []
        
        for resp in self.captured_responses:
            url = resp["url"]
            body = resp.get("body", "")
            
            # Skip non-data URLs
            if any(x in url.lower() for x in ["analytics", "tracking", "pixel", "ads", "facebook", "google"]):
                continue
            
            # Try to parse JSON
            try:
                data = json.loads(body)
                
                # Look for array data (company lists)
                data_path = self._find_data_array(data)
                if data_path:
                    endpoint = {
                        "url": url,
                        "method": "GET",
                        "content_type": resp.get("content_type", ""),
                        "data_path": data_path,
                        "sample_count": self._count_items(data, data_path),
                        "pagination_type": self._detect_pagination(data, url)
                    }
                    endpoints.append(endpoint)
                    logger.info(f"Found API endpoint: {url[:80]}... ({endpoint['sample_count']} items)")
                    
            except json.JSONDecodeError:
                pass
        
        return endpoints
    
    def _find_data_array(self, data: Any, path: str = "") -> Optional[str]:
        """JSON içinde veri dizisini bul."""
        if isinstance(data, list) and len(data) > 0:
            # Check if items look like company data
            if isinstance(data[0], dict):
                keys = set(data[0].keys())
                company_indicators = {"name", "company", "title", "firma", "unvan", "nome"}
                if keys & company_indicators:
                    return path or "root"
        
        if isinstance(data, dict):
            # Common data container keys
            for key in ["data", "items", "results", "records", "companies", "members", "list", "rows"]:
                if key in data:
                    result = self._find_data_array(data[key], f"{path}.{key}" if path else key)
                    if result:
                        return result
        
        return None
    
    def _count_items(self, data: Any, path: str) -> int:
        """Data path'teki item sayısını say."""
        if path == "root":
            return len(data) if isinstance(data, list) else 0
        
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return 0
        
        return len(current) if isinstance(current, list) else 0
    
    def _detect_pagination(self, data: Any, url: str) -> str:
        """Pagination tipini tespit et."""
        # URL'de pagination parametresi var mı?
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        
        if "page" in params or "p" in params:
            return "page"
        if "offset" in params or "start" in params:
            return "offset"
        if "cursor" in params or "after" in params:
            return "cursor"
        
        # JSON response'da pagination bilgisi var mı?
        if isinstance(data, dict):
            if "next_page" in data or "nextPage" in data:
                return "page"
            if "next_cursor" in data or "cursor" in data:
                return "cursor"
            if "total_pages" in data or "totalPages" in data:
                return "page"
        
        return "unknown"
    
    def _analyze_dom_structure(self, soup: BeautifulSoup) -> Dict:
        """DOM yapısını analiz et ve selector'ları belirle."""
        selectors = {
            "tables": [],
            "lists": [],
            "cards": [],
            "company_containers": []
        }
        
        # Tablo analizi
        for table in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            company_headers = ["company", "name", "firma", "şirket", "unvan", "empresa", "nome"]
            if any(h in " ".join(headers) for h in company_headers):
                rows = len(table.find_all("tr")) - 1
                selectors["tables"].append({
                    "selector": self._get_selector(table),
                    "headers": headers,
                    "row_count": rows
                })
        
        # Liste analizi (ul/ol with company-like content)
        for ul in soup.find_all(["ul", "ol"]):
            items = ul.find_all("li", recursive=False)
            if len(items) >= 5:  # At least 5 items
                text_sample = " ".join([li.get_text()[:100] for li in items[:3]])
                if self._looks_like_company_list(text_sample):
                    selectors["lists"].append({
                        "selector": self._get_selector(ul),
                        "item_count": len(items)
                    })
        
        # Card/Grid analizi (div containers with repeated structure)
        for container in soup.find_all("div", class_=re.compile(r"(grid|list|cards?|results?|items?)", re.I)):
            children = container.find_all("div", recursive=False)
            if len(children) >= 3:
                # Check if children have similar structure
                if self._has_similar_structure(children[:5]):
                    selectors["cards"].append({
                        "selector": self._get_selector(container),
                        "card_count": len(children)
                    })
        
        return selectors
    
    def _get_selector(self, element) -> str:
        """Element için CSS selector oluştur."""
        if element.get("id"):
            return f"#{element['id']}"
        
        classes = element.get("class", [])
        if classes:
            return f"{element.name}.{'.'.join(classes)}"
        
        return element.name
    
    def _looks_like_company_list(self, text: str) -> bool:
        """Metnin şirket listesi gibi görünüp görünmediğini kontrol et."""
        for pattern in self.COMPANY_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False
    
    def _has_similar_structure(self, elements: List) -> bool:
        """Elementlerin benzer yapıya sahip olup olmadığını kontrol et."""
        if len(elements) < 2:
            return False
        
        structures = []
        for el in elements:
            # Get child tag structure
            children = [c.name for c in el.find_all(recursive=False) if c.name]
            structures.append(tuple(children))
        
        # Check if at least 60% have the same structure
        from collections import Counter
        counter = Counter(structures)
        most_common = counter.most_common(1)
        if most_common:
            return most_common[0][1] / len(structures) >= 0.6
        
        return False
    
    def _extract_companies_from_dom(self, soup: BeautifulSoup, source_url: str) -> List[Dict]:
        """DOM'dan şirket bilgilerini çıkar."""
        companies = []
        seen_names = set()
        
        # Full text for pattern matching
        full_text = soup.get_text(" ", strip=True)
        
        # Extract company names
        for pattern in self.COMPANY_PATTERNS:
            matches = re.findall(pattern, full_text)
            for match in matches:
                name = match.strip()
                if len(name) > 3 and name.lower() not in seen_names:
                    seen_names.add(name.lower())
                    
                    company = {
                        "company": name,
                        "source_url": source_url,
                        "extraction_method": "dom_pattern"
                    }
                    
                    # Try to find context around the company name
                    context = self._find_context(soup, name)
                    if context:
                        company["context"] = context
                        company["country"] = self._detect_country(context)
                        company["email"] = self._extract_email(context)
                        company["website"] = self._extract_website(context)
                    
                    companies.append(company)
        
        return companies[:100]  # Limit to first 100
    
    def _find_context(self, soup: BeautifulSoup, company_name: str) -> str:
        """Şirket adı etrafındaki context'i bul."""
        # Find element containing company name
        for element in soup.find_all(string=re.compile(re.escape(company_name), re.I)):
            parent = element.parent
            if parent:
                # Get parent's text content
                context = parent.get_text(" ", strip=True)
                if len(context) > len(company_name):
                    return context[:500]  # Limit context length
        
        return ""
    
    def _detect_country(self, text: str) -> str:
        """Metinden ülke tespit et."""
        text_lower = text.lower()
        
        for country, indicators in self.COUNTRY_INDICATORS.items():
            for indicator in indicators:
                if indicator.lower() in text_lower:
                    return country
        
        return ""
    
    def _extract_email(self, text: str) -> str:
        """Metinden e-posta çıkar."""
        match = re.search(self.EMAIL_PATTERN, text)
        return match.group(0) if match else ""
    
    def _extract_website(self, text: str) -> str:
        """Metinden website çıkar."""
        match = re.search(self.WEBSITE_PATTERN, text)
        if match:
            domain = match.group(1)
            return f"https://www.{domain}"
        return ""
    
    def scrape_with_api(self, endpoint: Dict, max_pages: int = 10) -> List[Dict]:
        """API endpoint'i kullanarak veri çek."""
        all_data = []
        url = endpoint["url"]
        data_path = endpoint.get("data_path", "root")
        pagination_type = endpoint.get("pagination_type", "page")
        
        for page in range(1, max_pages + 1):
            # Build URL with pagination
            paginated_url = self._add_pagination(url, pagination_type, page)
            
            try:
                response = requests.get(paginated_url, timeout=30)
                if response.status_code != 200:
                    break
                
                data = response.json()
                items = self._extract_from_path(data, data_path)
                
                if not items:
                    break
                
                all_data.extend(items)
                logger.info(f"Page {page}: {len(items)} items (total: {len(all_data)})")
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"API scrape error: {e}")
                break
        
        return all_data
    
    def _add_pagination(self, url: str, pagination_type: str, page: int) -> str:
        """URL'ye pagination parametresi ekle."""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        
        if pagination_type == "page":
            params["page"] = [str(page)]
        elif pagination_type == "offset":
            params["offset"] = [str((page - 1) * 20)]
        
        from urllib.parse import urlencode
        new_query = urlencode(params, doseq=True)
        
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
    
    def _extract_from_path(self, data: Any, path: str) -> List:
        """JSON path'ten veri çıkar."""
        if path == "root":
            return data if isinstance(data, list) else []
        
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return []
        
        return current if isinstance(current, list) else []


class MultiLangSearcher:
    """
    Çok dilli arama motoru.
    
    Her dilde aynı kavramı arar ve sonuçları birleştirir.
    """
    
    def __init__(self, brave_api_key: str = None):
        import os
        self.api_key = brave_api_key or os.environ.get("BRAVE_API_KEY") or os.environ.get("Brave_API_KEY")
        self.http_client = HttpClient()
        
        # Load products config
        config_path = Path(__file__).parent.parent.parent / "config" / "products.yaml"
        if config_path.exists():
            import yaml
            with open(config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = {}
    
    def search_multilang(self, 
                         concept: str, 
                         countries: List[str] = None,
                         languages: List[str] = None,
                         max_results_per_query: int = 10) -> List[Dict]:
        """
        Çok dilli arama yap.
        
        Args:
            concept: Aranacak kavram (örn: "textile finishing")
            countries: Hedef ülkeler
            languages: Arama dilleri ("de", "en", "tr", "es", "pt")
        """
        if not self.api_key:
            logger.warning("Brave API key not set")
            return []
        
        languages = languages or ["de", "en", "tr", "es", "pt"]
        countries = countries or ["Turkey", "Brazil", "Egypt", "Morocco", "Argentina"]
        
        all_results = []
        seen_urls = set()
        
        # Build queries for each language
        queries = self._build_multilang_queries(concept, languages, countries)
        
        for query in queries:
            results = self._brave_search(query, max_results_per_query)
            
            for r in results:
                url = r.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    r["query_used"] = query
                    all_results.append(r)
            
            time.sleep(0.5)  # Rate limiting
        
        logger.info(f"Multi-lang search: {len(queries)} queries → {len(all_results)} unique URLs")
        return all_results
    
    def _build_multilang_queries(self, concept: str, languages: List[str], countries: List[str]) -> List[str]:
        """Çok dilli arama query'leri oluştur."""
        queries = []
        
        # Get templates from config
        templates = self.config.get("search_templates", {}).get("country_sector", {})
        
        for lang in languages:
            lang_templates = templates.get(lang, [])
            
            for country in countries:
                for template in lang_templates:
                    query = template.format(country=country)
                    queries.append(query)
        
        # Also add OEM + product queries
        products = self.config.get("products", [])
        for product in products[:5]:  # First 5 products
            for kw in product.get("search_keywords", [])[:2]:  # First 2 keywords per product
                queries.append(kw)
        
        return list(set(queries))[:50]  # Max 50 unique queries
    
    def _brave_search(self, query: str, count: int = 10) -> List[Dict]:
        """Brave Search API çağrısı."""
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
            if response.status_code == 200:
                data = response.json()
                results = data.get("web", {}).get("results", [])
                return results
        except Exception as e:
            logger.warning(f"Brave search error: {e}")
        
        return []


async def analyze_and_scrape(url: str) -> Dict:
    """
    URL'yi analiz et ve scrape et.
    
    Bu fonksiyon AutoScraper'ın ana entry point'i.
    """
    scraper = AutoScraper()
    
    # Analyze URL
    analysis = await scraper.analyze_url(url)
    
    # If API endpoint found, scrape with API
    if analysis["api_endpoints"]:
        for endpoint in analysis["api_endpoints"]:
            data = scraper.scrape_with_api(endpoint)
            analysis["scraped_data"] = data
            break  # Use first endpoint
    
    return analysis


if __name__ == "__main__":
    import asyncio
    
    # Test URL
    test_url = "https://www.oeko-tex.com/en/our-customers"
    
    print(f"Analyzing: {test_url}")
    result = asyncio.run(analyze_and_scrape(test_url))
    
    print(f"\nStrategy: {result['strategy']}")
    print(f"API Endpoints: {len(result['api_endpoints'])}")
    print(f"Companies Found: {len(result['companies_found'])}")
    
    if result['companies_found']:
        print("\nSample companies:")
        for c in result['companies_found'][:5]:
            print(f"  - {c.get('company', 'N/A')} ({c.get('country', 'N/A')})")
