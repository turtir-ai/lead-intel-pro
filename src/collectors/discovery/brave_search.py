import os
import time
from urllib.parse import urlparse

import requests

from src.utils.cache import load_json_cache, save_json_cache
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BraveSearchClient:
    def __init__(self, api_key=None, settings=None):
        settings = settings or {}
        # Get API key from parameter, env, or settings
        self.api_key = api_key or os.environ.get("BRAVE_API_KEY") or os.environ.get("Brave_API_KEY")
        self.base_url = settings.get("base_url", "https://api.search.brave.com/res/v1/web/search")
        self.timeout = settings.get("timeout", 30)
        self.delay = float(settings.get("delay", 1.0))
        self._last = 0

    def _rate_limit(self):
        if self.delay <= 0:
            return
        elapsed = time.time() - self._last
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    def search(self, query, count=10, offset=0):
        if not self.api_key:
            logger.warning("Brave API key missing; skipping discovery.")
            return []
        cache_key = f"brave:{query}:{count}:{offset}"
        cached = load_json_cache(cache_key)
        if cached:
            return cached.get("results", [])

        self._rate_limit()
        headers = {"X-Subscription-Token": self.api_key}
        params = {"q": query, "count": count, "offset": offset}
        try:
            resp = requests.get(self.base_url, headers=headers, params=params, timeout=self.timeout)
            if resp.status_code != 200:
                logger.error(f"Brave API error {resp.status_code}: {resp.text[:200]}")
                return []
            data = resp.json()
        except Exception as exc:
            logger.error(f"Brave API request failed: {exc}")
            return []
        finally:
            self._last = time.time()

        results = []
        for item in data.get("web", {}).get("results", []) or []:
            results.append(
                {
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "description": item.get("description", ""),
                }
            )

        save_json_cache(cache_key, {"results": results})
        return results


class SourceDiscovery:
    def __init__(self, search_client, disallow_domains=None):
        self.client = search_client
        self.disallow_domains = set((disallow_domains or []))

    def discover(self, queries, max_results=10):
        sources = {"fairs": [], "directories": []}
        seen_urls = set()
        for q in queries or []:
            qtext = q.get("query")
            qtype = q.get("type", "directories")
            if not qtext:
                continue
            results = self.client.search(qtext, count=max_results)
            for item in results:
                url = item.get("url") or ""
                if not url or url in seen_urls:
                    continue
                domain = urlparse(url).netloc.lower()
                if any(bad in domain for bad in self.disallow_domains):
                    continue
                seen_urls.add(url)
                sources[qtype].append(
                    {
                        "name": item.get("title") or domain,
                        "url": url,
                        "type": "html",
                        "enabled": True,
                        "discovered": True,
                    }
                )
        return sources
