from datetime import datetime

from bs4 import BeautifulSoup

from src.collectors.discovery.brave_search import BraveSearchClient
from src.processors.entity_extractor import EntityExtractor
from src.utils.evidence import record_evidence
from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


class EgyptTextileExportCouncil:
    def __init__(self, api_key, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.search = BraveSearchClient(api_key, settings=settings or {})
        self.client = HttpClient(settings=settings, policies=policies)
        self.extractor = EntityExtractor()
        self.evidence_path = evidence_path

    def harvest(self, search_query, max_results=30):
        leads = []
        if not search_query:
            return leads
        results = self.search.search(search_query, count=max_results)
        for res in results:
            url = res.get("url")
            if not url or "textile-egypt.org/textile-egypt.org/members" not in url:
                continue
            html = self.client.get(url)
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            rows = soup.find_all("tr")
            data = {}
            for row in rows:
                cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) >= 2:
                    key = cells[0].strip(":").lower()
                    value = " ".join(cells[1:]).strip()
                    data[key] = value
            company = data.get("company name") or data.get("commercial name") or ""
            if not company:
                # fallback: use page title
                title = soup.title.get_text(strip=True) if soup.title else ""
                company = title.split("|")[0].strip()
            context = "\n".join([f"{k}: {v}" for k, v in data.items()])

            content_hash = save_text_cache(url, context)
            record_evidence(
                self.evidence_path,
                {
                    "source_type": "directory",
                    "source_name": "Egypt_TEC",
                    "url": url,
                    "title": company,
                    "snippet": context[:400],
                    "content_hash": content_hash,
                    "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
            )

            emails = self.extractor.extract_emails(context)
            phones = self.extractor.extract_phones(context)
            websites = self.extractor.extract_websites(context)
            leads.append(
                {
                    "company": company,
                    "source": url,
                    "source_type": "egypt_tec",
                    "source_name": "Egypt_TEC",
                    "country": "Egypt",
                    "context": context[:2000],
                    "emails": emails,
                    "phones": phones,
                    "websites": websites,
                }
            )
        return leads
