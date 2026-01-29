from datetime import datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from src.processors.entity_extractor import EntityExtractor
from src.utils.evidence import record_evidence
from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


class TexbrasilCompanies:
    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.client = HttpClient(settings=settings, policies=policies)
        self.extractor = EntityExtractor()
        self.evidence_path = evidence_path

    def harvest(self, base_url="https://texbrasil.com.br", sitemap_url=None, max_pages=200, country="Brazil"):
        sitemap_url = sitemap_url or base_url.rstrip("/") + "/sitemap_index.xml"
        urls = self._fetch_sitemap_urls(sitemap_url)
        company_urls = []
        seen_slugs = set()
        for url in urls:
            if "/companies/" not in url or "/segments/" in url:
                continue
            if url.rstrip("/").endswith("/companies"):
                continue
            if "/es/companies/" in url:
                continue
            if "/en/companies/" not in url and "/pt/companies/" not in url:
                continue
            slug = url.split("/companies/")[-1].strip("/")
            if not slug or slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            company_urls.append(url)

        leads = []
        for url in company_urls[:max_pages]:
            html = self.client.get(url)
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            name = ""
            h1 = soup.find("h1")
            if h1:
                name = h1.get_text(" ", strip=True)
            if not name and soup.title:
                name = soup.title.get_text(" ", strip=True)
            if not name:
                continue

            text = soup.get_text(separator="\n", strip=True)
            websites = set(self.extractor.extract_websites(text))
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("http") and "texbrasil.com.br" not in href:
                    websites.add(href)

            website = ""
            if websites:
                website = sorted(websites)[0]

            snippet = text[:400].replace("\n", " ").strip()
            content_hash = save_text_cache(url, text[:5000])
            record_evidence(
                self.evidence_path,
                {
                    "source_type": "directory",
                    "source_name": "Texbrasil",
                    "url": url,
                    "title": name,
                    "snippet": snippet,
                    "content_hash": content_hash,
                    "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
            )

            leads.append(
                {
                    "company": name,
                    "country": country,
                    "source": url,
                    "source_type": "directory",
                    "source_name": "Texbrasil",
                    "website": website,
                    "context": snippet,
                }
            )

        logger.info(f"Texbrasil: harvested {len(leads)} companies")
        return leads

    def _fetch_sitemap_urls(self, sitemap_url):
        urls = []
        try:
            resp = requests.get(sitemap_url, timeout=20)
            if resp.status_code != 200:
                return urls
            xml = resp.text
        except Exception:
            return urls

        for line in xml.splitlines():
            line = line.strip()
            if line.startswith("<loc>") and line.endswith("</loc>"):
                loc = line.replace("<loc>", "").replace("</loc>", "").strip()
                if loc.endswith(".xml"):
                    urls.extend(self._fetch_sitemap_urls(loc))
                else:
                    urls.append(loc)
            elif "<loc>" in line:
                start = line.find("<loc>") + 5
                end = line.find("</loc>")
                if end > start:
                    loc = line[start:end].strip()
                    if loc.endswith(".xml"):
                        urls.extend(self._fetch_sitemap_urls(loc))
                    else:
                        urls.append(loc)

        # de-duplicate
        seen = set()
        deduped = []
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            deduped.append(url)
        return deduped
