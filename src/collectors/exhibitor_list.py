import re
from datetime import datetime

from bs4 import BeautifulSoup

from src.utils.evidence import record_evidence
from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


class ExhibitorListCollector:
    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.client = HttpClient(settings=settings, policies=policies)
        self.evidence_path = evidence_path

    def harvest(self, url, source_name, country="Brazil"):
        html = self.client.get(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        section = self._find_exhibitor_section(soup)
        headings = section.find_all(["h4", "h3"]) if section else soup.find_all(["h4", "h3"])

        leads = []
        for h in headings:
            name = h.get_text(" ", strip=True)
            if not name or len(name) < 2:
                continue
            details = []
            for sib in h.find_next_siblings():
                if sib.name in ("h3", "h4"):
                    break
                text = sib.get_text(" ", strip=True)
                if text:
                    details.append(text)
            detail_text = " ".join(details).strip()
            website = self._extract_website(detail_text, h)
            snippet = f"{name} | {detail_text}" if detail_text else name

            content_hash = save_text_cache(f"{url}#{name}", snippet)
            record_evidence(
                self.evidence_path,
                {
                    "source_type": "directory",
                    "source_name": source_name,
                    "url": url,
                    "title": name,
                    "snippet": snippet[:400],
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
                    "source_name": source_name,
                    "website": website,
                    "context": detail_text[:400],
                }
            )

        logger.info(f"{source_name}: harvested {len(leads)} exhibitors")
        return leads

    def _find_exhibitor_section(self, soup):
        header = soup.find(lambda tag: tag.name in ("h1", "h2", "h3") and "expositores" in tag.get_text(" ", strip=True).lower())
        if not header:
            return None
        # Try to find nearby accordion container
        container = header.find_next("div", class_=lambda c: c and "accordion" in " ".join(c))
        if container:
            return container
        return None

    def _extract_website(self, detail_text, heading):
        # Try to extract from text like "Site: example.com"
        if detail_text:
            match = re.search(r"(?:site|website)\\s*:\\s*([\\w.-]+(?:\\.[a-z]{2,})+)", detail_text, flags=re.IGNORECASE)
            if match:
                return match.group(1).strip()
        # Fallback: search for links within heading siblings
        for a in heading.find_all_next("a", href=True, limit=3):
            href = a["href"].strip()
            if href.startswith("http"):
                return href
        return ""
