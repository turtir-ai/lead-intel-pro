#!/usr/bin/env python3
"""
BRAVE SCENTER - V7 Autonomous Hunter Module

3-Phase Brave API Strategy:
1. Bulk Discovery - Find treasure files (PDFs with exhibitor lists)
2. Official Website Detection - Resolve directory URLs to real domains
3. Evidence Triangulation - Find stenter/OEM machine evidence

This module transforms Brave from a "search engine" to an "Evidence Detective".
"""

import os
import re
import time
import json
import hashlib
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse
from datetime import datetime

from src.utils.logger import get_logger

logger = get_logger(__name__)


# OEM Brands for evidence detection
OEM_BRANDS = [
    "monforts", "brückner", "bruckner", "krantz", "santex", "artos",
    "babcock", "goller", "benninger", "thies", "then", "jemco",
    "dilmenler", "comet", "erbatech", "montex", "proctor", "dmc"
]

# Stenter/Finishing keywords for evidence
STENTER_KEYWORDS = [
    # Turkish
    "ramöz", "ramoz", "ram makinesi", "germe makinesi", "stenter",
    "boyahane", "terbiye", "apre", "boya tesisi",
    # English
    "stenter", "stentering", "tenter frame", "heat setting", "finishing",
    "dyeing", "mercerizing", "sanforizing", "calendering",
    # Portuguese
    "rama", "ramas", "ramosa", "tinturaria", "acabamento", "alvejamento",
    # Spanish
    "rama", "ramas", "tintorería", "acabado", "blanqueo",
]

# Directory domains to skip (need real website discovery)
DIRECTORY_DOMAINS = [
    "oeko-tex.com", "gots.org", "bettercotton.org", "bluesign.com",
    "abit.org.br", "texbrasil.com.br", "linkedin.com", "facebook.com",
    "alibaba.com", "made-in-china.com", "indiamart.com", "tradeindia.com",
    "europages.", "kompass.com", "yellowpages", "yelp.com", "dnb.com",
    "bloomberg.com", "zoominfo.com", "crunchbase.com", "owler.com",
    "wikipedia.org", "twitter.com", "instagram.com", "youtube.com",
    "commonshare.com", "opensupplyhub.org", "nusalist.com", "mustakbil.com",
    "marketscreener.com", "investing.com", "reuters.com", "google.com",
]

# P1: Directory path patterns (even on valid domains, these paths indicate listings)
DIRECTORY_PATH_PATTERNS = [
    "/certificate/", "/certificates/", "/db/", "/database/",
    "/member/", "/members/", "/directory/", "/listing/",
    "/supplier/", "/suppliers/", "/company/", "/companies/",
    "/search", "/results", "/profile/", "/profiles/",
    "/exhibitor/", "/exhibitors/", "/participant/", "/participants/",
    "/buyer/", "/buyers/", "/seller/", "/sellers/",
    "/vendor/", "/vendors/", "/associate/", "/associates/",
]


class BraveScenter:
    """
    3-Phase Brave API Intelligence Gatherer.
    
    Phase 1: Bulk Discovery - Find PDF/Excel files with exhibitor lists
    Phase 2: Official Website Detection - Resolve directory URLs
    Phase 3: Evidence Triangulation - Find stenter/OEM evidence
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        cache_dir: str = "data/cache/brave",
        rate_limit_delay: float = 1.0,
    ):
        self.api_key = api_key or os.environ.get("BRAVE_API_KEY")
        if not self.api_key:
            logger.warning("BRAVE_API_KEY not set - scenting disabled")
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limit_delay = rate_limit_delay
        
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "X-Subscription-Token": self.api_key or "",
        })
        
        # Stats
        self.stats = {
            "queries_made": 0,
            "cache_hits": 0,
            "pdfs_found": 0,
            "websites_resolved": 0,
            "evidence_found": 0,
        }
    
    def _get_cache(self, key: str) -> Optional[Any]:
        """Get from file cache."""
        cache_file = self.cache_dir / f"{key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, "r") as f:
                    data = json.load(f)
                    if data.get("expires", 0) > time.time():
                        return data.get("value")
            except Exception:
                pass
        return None
    
    def _set_cache(self, key: str, value: Any, ttl: int = 86400 * 7) -> None:
        """Set to file cache."""
        cache_file = self.cache_dir / f"{key}.json"
        try:
            with open(cache_file, "w") as f:
                json.dump({"value": value, "expires": time.time() + ttl}, f)
        except Exception:
            pass
    
    def _search(self, query: str, count: int = 10) -> List[Dict]:
        """Execute Brave search with caching and rate limiting."""
        if not self.api_key:
            return []
        
        # Check cache
        cache_key = hashlib.md5(f"{query}:{count}".encode()).hexdigest()
        cached = self._get_cache(cache_key)
        if cached:
            self.stats["cache_hits"] += 1
            return cached
        
        try:
            response = self.session.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": query, "count": count},
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
            
            results = []
            for item in data.get("web", {}).get("results", []):
                results.append({
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "description": item.get("description", ""),
                    "domain": urlparse(item.get("url", "")).netloc,
                })
            
            # Cache results
            self._set_cache(cache_key, results)
            self.stats["queries_made"] += 1
            
            time.sleep(self.rate_limit_delay)
            return results
            
        except Exception as e:
            logger.error(f"Brave search error: {e}")
            return []
    
    # =========================================================================
    # PHASE 1: BULK DISCOVERY - Find treasure files
    # =========================================================================
    
    def phase1_bulk_discovery(
        self,
        region: str = "global",
        year_range: str = "2023..2026",
    ) -> List[Dict]:
        """
        Search for PDF/Excel files containing exhibitor lists.
        
        Returns list of potential bulk lead sources.
        """
        logger.info("Phase 1: Bulk Discovery - Searching for treasure files...")
        
        # Query templates for different regions
        queries = []
        
        if region in ["global", "latam", "brazil"]:
            queries.extend([
                f'filetype:pdf "lista de expositores" tinturaria {year_range}',
                f'filetype:pdf "exhibitor list" têxtil Brasil {year_range}',
                f'filetype:xlsx "empresas expositoras" têxtil {year_range}',
                f'site:.br filetype:pdf "feira" "têxtil" expositor',
            ])
        
        if region in ["global", "latam", "mexico"]:
            queries.extend([
                f'filetype:pdf "lista de expositores" textil México {year_range}',
                f'filetype:pdf "exhibitors" "textile" Mexico {year_range}',
            ])
        
        if region in ["global", "turkey"]:
            queries.extend([
                f'filetype:pdf "katılımcı listesi" tekstil {year_range}',
                f'filetype:pdf "exhibitor list" textile Turkey {year_range}',
                f'filetype:pdf "ITMA" "Turkish" exhibitor',
            ])
        
        if region in ["global", "pakistan", "asia"]:
            queries.extend([
                f'filetype:pdf "exhibitor list" textile Pakistan {year_range}',
                f'filetype:pdf "participant list" "textile" "dyeing" Asia',
            ])
        
        if region in ["global", "egypt", "africa"]:
            queries.extend([
                f'filetype:pdf "exhibitor" textile Egypt {year_range}',
                f'filetype:pdf "participant" textile "North Africa"',
            ])
        
        # General textile fair queries
        queries.extend([
            f'filetype:pdf "exhibitor list" "textile finishing" {year_range}',
            f'filetype:pdf "list of exhibitors" dyeing {year_range}',
        ])
        
        # Execute searches
        found_files = []
        seen_urls = set()
        
        for query in queries:
            results = self._search(query, count=10)
            
            for result in results:
                url = result.get("url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                
                # Check if it's a downloadable file
                url_str = url if isinstance(url, str) else ""
                if any(ext in url_str.lower() for ext in [".pdf", ".xlsx", ".xls", ".csv"]):
                    found_files.append({
                        "url": url,
                        "title": result.get("title", ""),
                        "description": result.get("description", ""),
                        "file_type": self._detect_file_type(url),
                        "query": query,
                        "discovered_at": datetime.now().isoformat(),
                    })
                    self.stats["pdfs_found"] += 1
        
        logger.info(f"Phase 1 complete: Found {len(found_files)} treasure files")
        return found_files
    
    def _detect_file_type(self, url: str) -> str:
        """Detect file type from URL."""
        if not isinstance(url, str):
            return "unknown"
        url_lower = url.lower()
        if ".pdf" in url_lower:
            return "pdf"
        elif ".xlsx" in url_lower:
            return "xlsx"
        elif ".xls" in url_lower:
            return "xls"
        elif ".csv" in url_lower:
            return "csv"
        return "unknown"
    
    # =========================================================================
    # PHASE 2: OFFICIAL WEBSITE DETECTION
    # =========================================================================
    
    def phase2_resolve_website(
        self,
        company: str,
        country: str = "",
        current_url: str = "",
    ) -> Optional[Dict]:
        """
        Resolve directory URL to official company website.
        
        Uses navigation queries to find real domain.
        """
        # Guard against NaN values
        if not isinstance(company, str):
            company = str(company) if company else ""
        if not isinstance(country, str):
            country = str(country) if country else ""
        if not isinstance(current_url, str):
            current_url = ""
        
        # Check if current URL is already a real website
        if current_url and not self._is_directory_url(current_url):
            return {"website": current_url, "source": "existing", "confidence": "high"}
        
        # Build search queries
        queries = [
            f'"{company}" official website -directory -association -facebook -linkedin',
            f'"{company}" {country} website contact',
        ]
        
        # Add country-specific domain search
        domain_map = {
            "turkey": ".com.tr", "türkiye": ".com.tr",
            "brazil": ".com.br", "brezilya": ".com.br",
            "pakistan": ".pk", "egypt": ".eg", "mısır": ".eg",
            "argentina": ".com.ar", "arjantin": ".com.ar",
            "mexico": ".mx", "meksika": ".mx",
            "peru": ".pe", "portugal": ".pt", "portekiz": ".pt",
        }
        
        country_lower = country.lower() if country else ""
        for key, domain in domain_map.items():
            if key in country_lower:
                queries.append(f'"{company}" site:{domain}')
                break
        
        # Execute searches
        for query in queries:
            results = self._search(query, count=5)
            
            for result in results:
                url = result.get("url", "")
                domain = result.get("domain", "")
                
                # Skip directory URLs
                if self._is_directory_url(url):
                    continue
                
                # Check if domain seems related to company name
                company_str = company if isinstance(company, str) else ""
                domain_str = domain if isinstance(domain, str) else ""
                company_parts = company_str.lower().split()[:2]
                domain_lower = domain_str.lower()
                
                # Match if any significant company word appears in domain
                for part in company_parts:
                    if len(part) > 3 and part in domain_lower:
                        self.stats["websites_resolved"] += 1
                        return {
                            "website": url,
                            "domain": domain,
                            "source": "brave_navigation",
                            "confidence": "medium",
                            "matched_on": part,
                        }
        
        return None
    
    def _is_directory_url(self, url: str) -> bool:
        """
        Check if URL is a directory/aggregator site.
        P1: Enhanced with path pattern detection.
        """
        # Guard against NaN/None/non-string values
        if not url or not isinstance(url, str):
            return False
        url_lower = url.lower()
        
        # Check blocked domains
        for blocked in DIRECTORY_DOMAINS:
            if blocked in url_lower:
                return True
        
        # P1: Check directory path patterns
        parsed = urlparse(url_lower)
        path = parsed.path
        for pattern in DIRECTORY_PATH_PATTERNS:
            if pattern in path:
                logger.debug(f"Directory path detected: {pattern} in {url}")
                return True
        
        return False
    
    # =========================================================================
    # PHASE 3: EVIDENCE TRIANGULATION
    # =========================================================================
    
    def phase3_find_evidence(
        self,
        company: str,
        website: str = "",
        country: str = "",
    ) -> Dict:
        """
        Search for stenter/OEM machine evidence.
        
        Returns evidence dict with signals and confidence.
        """
        # Guard against NaN values
        if not isinstance(website, str):
            website = ""
        if not isinstance(company, str):
            company = str(company) if company else ""
        
        evidence = {
            "has_oem_evidence": False,
            "has_stenter_evidence": False,
            "oem_brands": [],
            "stenter_signals": [],
            "snippets": [],
            "evidence_details": [],  # P2: Detailed evidence with context
            "sce_score": 0.0,
            "sce_sales_ready": False,
        }
        
        # Query 1: Site-specific search (if website available)
        if website:
            domain = urlparse(website).netloc
            if domain:
                site_query = f'site:{domain} "stenter" OR "ramöz" OR "rama" OR "finishing" OR "dyeing"'
                results = self._search(site_query, count=5)
                
                for result in results:
                    snippet = result.get("description", "")
                    url = result.get("url", website)
                    evidence["snippets"].append(snippet)
                    self._analyze_snippet(snippet, evidence, source_url=url)
        
        # Query 2: OEM brand search
        oem_query = f'"{company}" "Monforts" OR "Brückner" OR "Krantz" OR "Santex" OR "installed"'
        results = self._search(oem_query, count=5)
        
        for result in results:
            snippet = result.get("description", "")
            url = result.get("url", "")
            evidence["snippets"].append(snippet)
            self._analyze_snippet(snippet, evidence, source_url=url)
        
        # Query 3: General stenter search
        stenter_query = f'"{company}" "stenter" OR "ramöz" OR "rama" OR "finishing plant"'
        results = self._search(stenter_query, count=5)
        
        for result in results:
            snippet = result.get("description", "")
            url = result.get("url", "")
            evidence["snippets"].append(snippet)
            self._analyze_snippet(snippet, evidence, source_url=url)
        
        # Calculate SCE score
        evidence["sce_score"] = self._calculate_sce_score(evidence)
        evidence["sce_sales_ready"] = evidence["sce_score"] >= 0.5
        
        if evidence["sce_sales_ready"]:
            self.stats["evidence_found"] += 1
        
        return evidence
    
    def _analyze_snippet(self, snippet: str, evidence: Dict, source_url: str = "") -> None:
        """
        Analyze snippet for OEM and stenter signals.
        P2: Now captures context window with matched terms.
        """
        if not snippet:
            return
        
        # Guard against NaN values
        if not isinstance(snippet, str):
            snippet = str(snippet) if snippet else ""
        
        snippet_lower = snippet.lower()
        
        # Check OEM brands
        for brand in OEM_BRANDS:
            if brand in snippet_lower:
                if brand not in evidence["oem_brands"]:
                    evidence["oem_brands"].append(brand)
                    evidence["has_oem_evidence"] = True
                    # P2: Capture context window
                    context = self._extract_context_window(snippet, brand)
                    if context:
                        evidence.setdefault("evidence_details", []).append({
                            "type": "oem_brand",
                            "term": brand,
                            "context": context,
                            "url": source_url,
                        })
        
        # Check stenter keywords
        for keyword in STENTER_KEYWORDS:
            if keyword in snippet_lower:
                if keyword not in evidence["stenter_signals"]:
                    evidence["stenter_signals"].append(keyword)
                    evidence["has_stenter_evidence"] = True
                    # P2: Capture context window
                    context = self._extract_context_window(snippet, keyword)
                    if context:
                        evidence.setdefault("evidence_details", []).append({
                            "type": "stenter_keyword",
                            "term": keyword,
                            "context": context,
                            "url": source_url,
                        })
    
    def _extract_context_window(self, text: str, term: str, window: int = 150) -> str:
        """
        P2: Extract context window around a matched term.
        Returns ~300 chars centered on the term.
        """
        # Guard against NaN values
        if not isinstance(text, str):
            text = str(text) if text else ""
        if not isinstance(term, str):
            term = str(term) if term else ""
        
        text_lower = text.lower()
        term_lower = term.lower()
        
        pos = text_lower.find(term_lower)
        if pos == -1:
            return ""
        
        start = max(0, pos - window)
        end = min(len(text), pos + len(term) + window)
        
        context = text[start:end].strip()
        
        # Clean up - remove excessive whitespace
        context = " ".join(context.split())
        
        # Add ellipsis if truncated
        if start > 0:
            context = "..." + context
        if end < len(text):
            context = context + "..."
        
        return context
    
    def _calculate_sce_score(self, evidence: Dict) -> float:
        """
        Calculate SCE (Stenter Customer Evidence) score.
        P1: Enhanced with proximity bonus.
        """
        score = 0.0
        
        # OEM brands = strong signal (0.4)
        if evidence["oem_brands"]:
            score += 0.3 + (0.1 * min(len(evidence["oem_brands"]), 3))
        
        # Stenter keywords = medium signal (0.3)
        if evidence["stenter_signals"]:
            score += 0.2 + (0.05 * min(len(evidence["stenter_signals"]), 4))
        
        # Both = strong combined signal
        if evidence["has_oem_evidence"] and evidence["has_stenter_evidence"]:
            score += 0.2
        
        # P1: Proximity bonus - OEM and keyword in same snippet
        evidence_details = evidence.get("evidence_details", [])
        if len(evidence_details) >= 2:
            oem_contexts = [e["context"] for e in evidence_details if e["type"] == "oem_brand"]
            kw_contexts = [e["context"] for e in evidence_details if e["type"] == "stenter_keyword"]
            
            # Check if any OEM brand appears near a stenter keyword
            for oem_ctx in oem_contexts:
                for kw_ctx in kw_contexts:
                    # Guard against NaN values
                    oem_ctx_str = oem_ctx if isinstance(oem_ctx, str) else ""
                    # If contexts overlap or are from same snippet
                    if any(kw in oem_ctx_str.lower() for kw in evidence["stenter_signals"]):
                        score += 0.1  # Proximity bonus
                        break
        
        return min(score, 1.0)
    
    # =========================================================================
    # UNIFIED SCENTING - Run all phases on a lead
    # =========================================================================
    
    def scent_lead(self, lead: Dict) -> Dict:
        """
        Run full scenting pipeline on a single lead.
        
        1. Resolve official website (Phase 2)
        2. Find evidence (Phase 3)
        
        Returns enriched lead dict.
        """
        company = lead.get("company", "")
        country = lead.get("country", "")
        website = lead.get("website", "")
        
        # Guard against NaN values from pandas
        if not isinstance(website, str):
            website = ""
        if not isinstance(company, str):
            company = str(company) if company else ""
        
        if not company:
            return lead
        
        # Phase 2: Resolve website
        if not website or self._is_directory_url(website):
            resolved = self.phase2_resolve_website(company, country, website)
            if resolved:
                lead["website"] = resolved.get("website", "")
                lead["website_source"] = resolved.get("source", "")
                lead["website_confidence"] = resolved.get("confidence", "")
        
        # Get website for Phase 3 (guard against NaN)
        current_website = lead.get("website", "")
        if not isinstance(current_website, str):
            current_website = ""
        
        # Phase 3: Find evidence
        evidence = self.phase3_find_evidence(
            company=company,
            website=current_website,
            country=country,
        )
        
        # Merge evidence into lead
        lead["oem_brands"] = evidence.get("oem_brands", [])
        lead["stenter_signals"] = evidence.get("stenter_signals", [])
        lead["sce_score"] = evidence.get("sce_score", 0.0)
        lead["sce_sales_ready"] = evidence.get("sce_sales_ready", False)
        lead["evidence_snippets"] = evidence.get("snippets", [])[:3]  # Keep top 3
        
        return lead
    
    def scent_leads_batch(
        self,
        leads: List[Dict],
        progress_callback: Optional[callable] = None,
    ) -> List[Dict]:
        """
        Run scenting on a batch of leads.
        
        Returns enriched leads list.
        """
        logger.info(f"Scenting {len(leads)} leads...")
        
        enriched = []
        for i, lead in enumerate(leads):
            enriched_lead = self.scent_lead(lead)
            enriched.append(enriched_lead)
            
            if progress_callback:
                progress_callback(i + 1, len(leads))
            elif (i + 1) % 10 == 0:
                logger.info(f"Scenting progress: {i + 1}/{len(leads)}")
        
        # Log stats
        logger.info(f"Scenting complete. Stats: {self.stats}")
        
        return enriched
    
    def get_stats(self) -> Dict:
        """Return scenting statistics."""
        return self.stats.copy()


# Legacy compatibility
def scent_region(region: str, limit: int = 10):
    """Legacy function for backward compatibility."""
    scenter = BraveScenter()
    return scenter.phase1_bulk_discovery(region=region)
