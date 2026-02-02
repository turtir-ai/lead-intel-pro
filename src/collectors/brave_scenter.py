
import os
import json
import time
import yaml
import logging
import requests
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BraveScenter:
    """
    'Scenting' engine: Finds high-quality seed URLs (Associations, Fairs, Member Lists)
    using Brave Search API with region-specific query templates.
    """
    
    def __init__(self, output_dir: str = "data/seeds"):
        self.api_key = os.getenv("BRAVE_API_KEY")
        self.base_url = "https://api.search.brave.com/res/v1/web/search"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load templates from config
        self.config_path = self.output_dir.parent.parent / "config" / "source_registry.yaml"
        self.templates = self._load_templates()

    def _load_templates(self) -> Dict:
        """Load query templates from YAML."""
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    return config.get("regions", {})
            except Exception as e:
                logger.error(f"Failed to load source registry: {e}")
        
        # Fallback defaults if config missing
        logger.warning("Using fallback templates (config not found)")
        return {
            "south_america": {
                "fairs": ["site:.br feira textil expositores"],
                "associations": ["site:.br associacao textil membros"],
                "directories": ["site:.br diretorio textil"]
            }
        }

    def scent_region(self, region: str, limit: int = 10):
        """
        Run scenting queries for a specific region.
        
        Args:
            region: 'south_america', 'north_africa', 'turkey', 'europe'
            limit: Max results per query
        """
        if region not in self.templates:
            logger.warning(f"Region {region} not found in templates.")
            return

        if not self.api_key:
            logger.warning("BRAVE_API_KEY not set - skipping scenting.")
            return

        logger.info(f"👃 Scenting region: {region}...")
        
        for category, queries in self.templates[region].items():
            results = []
            for query in queries:
                logger.info(f"  🔍 Query: {query}")
                search_res = self._search(query, count=limit)
                
                # Tag results
                for idx, item in enumerate(search_res, start=1):
                    url = item.get("url") or item.get("link") or ""
                    title = item.get("title") or ""
                    snippet = item.get("description") or item.get("snippet") or ""
                    results.append({
                        "country": self._infer_country(query, url),
                        "region": region,
                        "category": category,
                        "query": query,
                        "url": url,
                        "title": title,
                        "snippet": snippet,
                        "rank": item.get("rank") or idx,
                        "retrieved_at": datetime.now().isoformat(),
                        "source": "brave_search",
                    })
                
                # Be polite to API
                time.sleep(1.0)
            
            # Save results per category
            self._save_results(region, category, results)

    def _search(self, query: str, count: int = 10) -> List[Dict]:
        """Execute query against Brave Search API."""
        try:
            headers = {
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "X-Subscription-Token": self.api_key
            }
            params = {"q": query, "count": count}
            
            resp = requests.get(self.base_url, params=params, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
        return data.get("web", {}).get("results", [])

    def _infer_country(self, query: str, url: str) -> str:
        tld_map = {
            ".br": "Brazil",
            ".ar": "Argentina",
            ".co": "Colombia",
            ".pe": "Peru",
            ".mx": "Mexico",
            ".eg": "Egypt",
            ".ma": "Morocco",
            ".tn": "Tunisia",
            ".tr": "Turkey",
        }

        q = (query or "").lower()
        for tld, country in tld_map.items():
            if f"site:{tld}" in q or f"site:*.{tld.lstrip('.')}" in q or f"site:.{tld.lstrip('.')}" in q:
                return country

        if url:
            for tld, country in tld_map.items():
                if url.lower().endswith(tld) or f".{tld.lstrip('.')}/" in url.lower():
                    return country

        # Country name keywords in query
        name_map = {
            "brazil": "Brazil",
            "brasil": "Brazil",
            "argentina": "Argentina",
            "colombia": "Colombia",
            "peru": "Peru",
            "mexico": "Mexico",
            "mexico": "Mexico",
            "egypt": "Egypt",
            "morocco": "Morocco",
            "tunisia": "Tunisia",
            "turkey": "Turkey",
            "türkiye": "Turkey",
        }
        for key, country in name_map.items():
            if key in q:
                return country
        return ""
            else:
                logger.error(f"Brave API Error {resp.status_code}: {resp.text}")
                return []
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

    def _save_results(self, region: str, category: str, results: List[Dict]):
        """Save unique results to JSONL."""
        if not results:
            return

        filename = self.output_dir / f"{region}_{category}_seeds.jsonl"
        
        # Load existing to dedupe
        seen_urls = set()
        if filename.exists():
            with open(filename, 'r') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        seen_urls.add(data.get('url'))
                    except: pass
        
        new_count = 0
        with open(filename, 'a') as f:
            for item in results:
                if item.get('url') not in seen_urls:
                    f.write(json.dumps(item) + "\n")
                    seen_urls.add(item.get('url'))
                    new_count += 1
        
        logger.info(f"  💾 Saved {new_count} new seeds to {filename}")

if __name__ == "__main__":
    # Test run
    scenter = BraveScenter()
    scenter.scent_region("south_america", limit=5)
