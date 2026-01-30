"""
Brave Search API Discovery Module

Uses Brave Search API to find new potential B2B lead sources.
Searches for textile industry directories, member lists, etc.

Note: Requires BRAVE_API_KEY environment variable.
Free tier: 2,000 queries/month
"""

import os
import re
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urlparse, quote_plus
from datetime import datetime

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


class BraveDiscoverer:
    """
    Discover new B2B lead sources using Brave Search API.
    
    Brave Search API is chosen because:
    1. Privacy-focused (no tracking)
    2. Free tier available (2,000 queries/month)
    3. Good for finding directories and member lists
    4. Returns structured results
    """
    
    BASE_URL = "https://api.search.brave.com/res/v1/web/search"
    
    # Pre-defined search queries for textile industry
    SEARCH_TEMPLATES = [
        # Association member directories
        '"{country}" textile association member directory',
        '"{country}" textile manufacturers directory',
        '"{country}" garment exporters list',
        '"{country}" fabric suppliers directory',
        
        # Trade fair exhibitors
        '"{country}" textile fair exhibitor list',
        '"{country}" textile exhibition companies',
        
        # Certification directories
        'OEKO-TEX certified companies "{country}"',
        'GOTS certified textile "{country}"',
        'bluesign partner "{country}"',
        
        # Industry portals
        '"{country}" textile industry portal companies',
        '"{country}" spinning weaving dyeing companies',
        
        # Trade data
        '"{country}" cotton yarn export companies',
        '"{country}" textile machinery manufacturers',
    ]
    
    # Target countries for B2B leads
    TARGET_COUNTRIES = [
        "Egypt", "Morocco", "Tunisia", "Algeria",
        "Brazil", "Argentina", "Colombia", "Peru", "Mexico",
        "Turkey", "Pakistan", "Bangladesh", "India", "Vietnam",
    ]
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("BRAVE_API_KEY")
        self.cache_dir = Path("data/raw/json/brave_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Track discovered URLs
        self.discovered_sources_path = Path("data/staging/discovered_sources.yaml")
        
    def _get_cache_key(self, query: str) -> str:
        """Generate cache key for query."""
        return hashlib.md5(query.encode()).hexdigest()
    
    def _load_cache(self, query: str) -> Optional[Dict]:
        """Load cached search results."""
        cache_file = self.cache_dir / f"{self._get_cache_key(query)}.json"
        if cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Check if cache is less than 7 days old
                    cached_at = datetime.fromisoformat(data.get("cached_at", "2000-01-01"))
                    if (datetime.utcnow() - cached_at).days < 7:
                        return data
            except Exception:
                pass
        return None
    
    def _save_cache(self, query: str, data: Dict):
        """Save search results to cache."""
        cache_file = self.cache_dir / f"{self._get_cache_key(query)}.json"
        data["cached_at"] = datetime.utcnow().isoformat()
        data["query"] = query
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def search(self, query: str, count: int = 10) -> List[Dict]:
        """
        Search Brave for a query.
        Returns list of search results with url, title, description.
        """
        if not self.api_key:
            logger.warning("BRAVE_API_KEY not set. Using mock results for testing.")
            return self._mock_search(query)
        
        # Check cache first
        cached = self._load_cache(query)
        if cached:
            logger.info(f"Brave: using cached results for '{query[:50]}...'")
            return cached.get("results", [])
        
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": self.api_key,
        }
        
        params = {
            "q": query,
            "count": count,
            "search_lang": "en",
            "country": "all",
            "safesearch": "off",
        }
        
        try:
            resp = requests.get(self.BASE_URL, headers=headers, params=params, timeout=30)
            
            if resp.status_code == 401:
                logger.error("Brave API: Invalid API key")
                return []
            elif resp.status_code == 429:
                logger.error("Brave API: Rate limit exceeded")
                return []
            elif resp.status_code != 200:
                logger.error(f"Brave API: HTTP {resp.status_code}")
                return []
            
            data = resp.json()
            results = []
            
            for item in data.get("web", {}).get("results", []):
                results.append({
                    "url": item.get("url", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "age": item.get("age", ""),
                })
            
            # Cache results
            self._save_cache(query, {"results": results})
            
            logger.info(f"Brave: found {len(results)} results for '{query[:50]}...'")
            return results
            
        except Exception as e:
            logger.error(f"Brave API error: {e}")
            return []
    
    def _mock_search(self, query: str) -> List[Dict]:
        """Mock search results for testing without API key."""
        return [
            {
                "url": "https://example.com/textile-directory",
                "title": f"Textile Directory - {query}",
                "description": "Mock result for testing",
            }
        ]
    
    def discover_sources(self, countries: Optional[List[str]] = None, 
                         max_queries: int = 50) -> List[Dict]:
        """
        Discover new B2B lead sources by searching for directories.
        Returns list of potential sources with metadata.
        """
        countries = countries or self.TARGET_COUNTRIES
        discovered = []
        seen_domains = set()
        query_count = 0
        
        for country in countries:
            if query_count >= max_queries:
                break
                
            for template in self.SEARCH_TEMPLATES:
                if query_count >= max_queries:
                    break
                
                query = template.format(country=country)
                results = self.search(query)
                query_count += 1
                
                for result in results:
                    url = result.get("url", "")
                    domain = urlparse(url).netloc.lower()
                    
                    # Skip already seen domains
                    if domain in seen_domains:
                        continue
                    seen_domains.add(domain)
                    
                    # Score the result for relevance
                    score = self._score_result(result, country)
                    
                    if score >= 3:
                        discovered.append({
                            "url": url,
                            "domain": domain,
                            "title": result.get("title", ""),
                            "description": result.get("description", ""),
                            "country": country,
                            "query": query,
                            "score": score,
                            "discovered_at": datetime.utcnow().isoformat(),
                        })
        
        # Sort by score
        discovered.sort(key=lambda x: x["score"], reverse=True)
        
        logger.info(f"Brave: discovered {len(discovered)} potential sources")
        return discovered
    
    def _score_result(self, result: Dict, country: str) -> int:
        """Score a search result for B2B lead relevance."""
        score = 0
        
        text = (result.get("title", "") + " " + result.get("description", "")).lower()
        url = result.get("url", "").lower()
        
        # Positive signals
        positive_keywords = [
            ("directory", 2), ("member", 2), ("list", 1),
            ("association", 2), ("exhibitor", 2), ("company", 1),
            ("manufacturer", 2), ("supplier", 2), ("exporter", 2),
            ("textile", 2), ("fabric", 1), ("garment", 1),
            ("certified", 1), ("oeko-tex", 2), ("gots", 2),
        ]
        
        for kw, points in positive_keywords:
            if kw in text or kw in url:
                score += points
        
        # Country match bonus
        if country.lower() in text:
            score += 2
        
        # Negative signals (skip these)
        negative_keywords = ["linkedin", "facebook", "twitter", "youtube", 
                           "wikipedia", "news", "blog", "article"]
        for kw in negative_keywords:
            if kw in url:
                score -= 5
        
        return score
    
    def save_discovered_sources(self, sources: List[Dict]):
        """Save discovered sources to YAML for manual review."""
        import yaml
        
        # Load existing sources
        existing = {}
        if self.discovered_sources_path.exists():
            with open(self.discovered_sources_path, "r", encoding="utf-8") as f:
                existing = yaml.safe_load(f) or {}
        
        # Add new sources
        for source in sources:
            domain = source["domain"]
            if domain not in existing:
                existing[domain] = {
                    "url": source["url"],
                    "title": source["title"],
                    "country": source["country"],
                    "score": source["score"],
                    "status": "pending",  # pending, diagnosed, integrated, rejected
                    "discovered_at": source["discovered_at"],
                }
        
        # Save
        with open(self.discovered_sources_path, "w", encoding="utf-8") as f:
            yaml.dump(existing, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"Saved {len(existing)} sources to {self.discovered_sources_path}")


# CLI usage
if __name__ == "__main__":
    import sys
    
    discoverer = BraveDiscoverer()
    
    if len(sys.argv) > 1:
        # Search specific query
        query = " ".join(sys.argv[1:])
        results = discoverer.search(query)
        print(f"\nSearch: {query}")
        print("="*60)
        for r in results:
            print(f"  {r['title'][:60]}")
            print(f"  {r['url']}")
            print()
    else:
        # Full discovery
        sources = discoverer.discover_sources(
            countries=["Egypt", "Morocco"],
            max_queries=10
        )
        print(f"\nDiscovered {len(sources)} potential sources:")
        for s in sources[:10]:
            print(f"  [{s['score']}] {s['title'][:50]}")
            print(f"      {s['url']}")
        
        discoverer.save_discovered_sources(sources)
