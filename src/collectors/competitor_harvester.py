from collections import deque
from datetime import datetime
from urllib.parse import urljoin, urlparse

import trafilatura
from bs4 import BeautifulSoup

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache
from src.utils.evidence import record_evidence

logger = get_logger(__name__)

class CompetitorHarvester:
    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.client = HttpClient(settings=settings, policies=policies)
        self.evidence_path = evidence_path

    def _same_domain(self, base_url, other_url):
        return urlparse(base_url).netloc == urlparse(other_url).netloc

    def _is_relevant_url(self, url, search_paths, include_keywords):
        url_l = url.lower()
        if any(path.lower() in url_l for path in search_paths):
            return True
        if any(kw in url_l for kw in include_keywords):
            return True
        return False

    def _extract_text(self, html):
        text = trafilatura.extract(html) or ""
        if not text.strip():
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text(separator="\n", strip=True)
        return text

    def _is_binary(self, url):
        url_l = url.lower()
        for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".pdf", ".zip", ".rar", ".mp4", ".mp3"):
            if url_l.endswith(ext):
                return True
        return False

    def _fetch_sitemap_links(self, base_url, sitemap_url=None, limit=200):
        candidates = []
        visited_sitemaps = set()
        sitemap_queue = []

        if sitemap_url:
            sitemap_queue.append(sitemap_url)
        else:
            sitemap_queue.append(base_url.rstrip("/") + "/sitemap.xml")
            sitemap_queue.append(base_url.rstrip("/") + "/sitemap_index.xml")

        while sitemap_queue and len(candidates) < limit:
            sm_url = sitemap_queue.pop(0)
            if sm_url in visited_sitemaps:
                continue
            visited_sitemaps.add(sm_url)
            xml = self.client.get(sm_url)
            if not xml:
                continue

            urls = []
            for line in xml.splitlines():
                line = line.strip()
                if line.startswith("<loc>") and line.endswith("</loc>"):
                    loc = line.replace("<loc>", "").replace("</loc>", "").strip()
                    urls.append(loc)
                elif "<loc>" in line:
                    start = line.find("<loc>") + 5
                    end = line.find("</loc>")
                    if end > start:
                        urls.append(line[start:end].strip())

            for loc in urls:
                if loc.endswith(".xml") and "sitemap" in loc:
                    sitemap_queue.append(loc)
                else:
                    candidates.append(loc)

        return candidates

    def harvest_competitor(self, competitor_config):
        base_url = competitor_config['url']
        search_paths = competitor_config.get('search_paths', [])
        include_keywords = competitor_config.get(
            "include_keywords",
            ["references", "referenzen", "kunden", "clients", "projects", "news", "aktuelles"],
        )
        exclude_paths = competitor_config.get("exclude_paths", [])
        max_pages = int(competitor_config.get("max_pages", 30))
        max_depth = int(competitor_config.get("max_depth", 2))
        sitemap_url = competitor_config.get("sitemap_url")

        all_pages = []
        visited = set()
        queue = deque([(base_url, 0)])

        # Seed queue with sitemap URLs (if available)
        sitemap_links = self._fetch_sitemap_links(base_url, sitemap_url=sitemap_url, limit=200)
        for link in sitemap_links:
            if self._same_domain(base_url, link) and self._is_relevant_url(link, search_paths, include_keywords):
                if link not in visited:
                    queue.append((link, 1))

        while queue and len(visited) < max_pages:
            url, depth = queue.popleft()
            if url in visited:
                continue
            if not self._same_domain(base_url, url):
                continue
            if any(excl in url.lower() for excl in exclude_paths):
                continue
            if self._is_binary(url):
                continue

            html = self.client.get(url)
            if not html:
                continue
            visited.add(url)

            soup = BeautifulSoup(html, "html.parser")
            title = soup.title.get_text(strip=True) if soup.title else ""
            text = self._extract_text(html)
            if text.strip():
                if title and title not in text:
                    text = f"{title}\n{text}"
                content_hash = save_text_cache(url, text)
                snippet = text[:400].replace("\n", " ").strip()
                record_evidence(
                    self.evidence_path,
                    {
                        "source_type": "competitor",
                        "source_name": competitor_config.get("name", ""),
                        "url": url,
                        "title": title,
                        "snippet": snippet,
                        "content_hash": content_hash,
                        "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                    },
                )
                all_pages.append({"url": url, "title": title, "content": text, "snippet": snippet})

            if depth >= max_depth:
                continue

            for link in soup.find_all("a", href=True):
                href = link["href"].strip()
                if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                    continue
                full_url = urljoin(base_url, href)
                if not self._same_domain(base_url, full_url):
                    continue
                if self._is_relevant_url(full_url, search_paths, include_keywords) or depth == 0:
                    if full_url not in visited:
                        queue.append((full_url, depth + 1))

        return all_pages
