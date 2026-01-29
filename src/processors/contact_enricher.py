from datetime import datetime
from urllib.parse import urljoin, urlparse

import copy

from bs4 import BeautifulSoup

from src.processors.entity_extractor import EntityExtractor
from src.utils.evidence import record_evidence
from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


class ContactEnricher:
    def __init__(
        self,
        settings=None,
        policies=None,
        contact_cfg=None,
        evidence_path="outputs/evidence/evidence_log.csv",
    ):
        settings = settings or {}
        self.cfg = contact_cfg or settings.get("contact", {})
        local_settings = copy.deepcopy(settings)
        crawler_cfg = local_settings.setdefault("crawler", {})
        crawler_cfg["timeout"] = int(self.cfg.get("timeout", crawler_cfg.get("timeout", 15)))
        crawler_cfg["max_retries"] = int(self.cfg.get("max_retries", crawler_cfg.get("max_retries", 1)))
        self.client = HttpClient(settings=local_settings, policies=policies)
        self.extractor = EntityExtractor()
        self.evidence_path = evidence_path
        self._domain_cache = {}

    def enrich(self, lead):
        if not self.cfg.get("enabled", False):
            return lead

        source_types = set(self.cfg.get("target_source_types", []) or [])
        if source_types and lead.get("source_type") not in source_types:
            return lead

        websites = list(lead.get("websites") or [])
        if lead.get("website"):
            websites.insert(0, lead.get("website"))
        websites = [self._normalize_url(w) for w in websites if w]
        websites = [w for w in websites if w]
        if not websites:
            return lead

        emails = set(lead.get("emails") or [])
        phones = set(lead.get("phones") or [])
        contact_urls = set(lead.get("contact_urls") or [])

        max_pages = int(self.cfg.get("max_pages", 4))
        paths = self.cfg.get("paths") or ["/contact", "/contact-us"]
        keywords = [k.lower() for k in (self.cfg.get("keywords") or [])]

        for base in websites:
            domain = urlparse(base).netloc.lower()
            if not domain:
                continue
            if domain in self._domain_cache:
                cached = self._domain_cache[domain]
                emails.update(cached.get("emails", []))
                phones.update(cached.get("phones", []))
                contact_urls.update(cached.get("contact_urls", []))
                continue

            found = {"emails": set(), "phones": set(), "contact_urls": set()}
            base_html = self.client.get(base)
            if not base_html:
                continue

            candidates = [base]
            for path in paths:
                candidates.append(urljoin(base.rstrip("/") + "/", path.lstrip("/")))
            if keywords:
                candidates.extend(self._find_contact_links(base_html, base, keywords))

            # de-duplicate while preserving order
            seen = set()
            deduped = []
            for url in candidates:
                if url in seen:
                    continue
                seen.add(url)
                deduped.append(url)
            candidates = deduped

            for url in candidates[:max_pages]:
                html = base_html if url == base else self.client.get(url)
                if not html:
                    continue
                text = self._html_to_text(html)
                found["emails"].update(self.extractor.extract_emails(text))
                found["phones"].update(self.extractor.extract_phones(text))
                found["emails"].update(self._extract_mailto(html))
                found["phones"].update(self._extract_tel(html))
                found["contact_urls"].add(url)

                content_hash = save_text_cache(url, text[:5000])
                record_evidence(
                    self.evidence_path,
                    {
                        "source_type": "contact_enrichment",
                        "source_name": lead.get("company", ""),
                        "url": url,
                        "title": lead.get("company", ""),
                        "snippet": text[:400].replace("\n", " ").strip(),
                        "content_hash": content_hash,
                        "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                    },
                )

            self._domain_cache[domain] = {
                "emails": found["emails"],
                "phones": found["phones"],
                "contact_urls": found["contact_urls"],
            }
            emails.update(found["emails"])
            phones.update(found["phones"])
            contact_urls.update(found["contact_urls"])

        if emails:
            lead["emails"] = sorted(emails)
        if phones:
            lead["phones"] = sorted(phones)
        if contact_urls:
            lead["contact_urls"] = sorted(contact_urls)
            lead["contact_page_found"] = True

        return lead

    def _normalize_url(self, url):
        url = str(url).strip()
        if not url:
            return ""
        if url.lower() in {"nan", "none", "null"}:
            return ""
        if not url.startswith("http://") and not url.startswith("https://"):
            url = f"https://{url}"
        return url

    def _html_to_text(self, html):
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator="\n", strip=True)

    def _extract_mailto(self, html):
        emails = set()
        soup = BeautifulSoup(html, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"].strip()
            if href.lower().startswith("mailto:"):
                addr = href.split(":", 1)[1].split("?")[0]
                if addr:
                    emails.add(addr)
        return emails

    def _extract_tel(self, html):
        phones = set()
        soup = BeautifulSoup(html, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"].strip()
            if href.lower().startswith("tel:"):
                phone = href.split(":", 1)[1].split("?")[0]
                if phone:
                    phones.add(phone)
        return phones

    def _find_contact_links(self, html, base_url, keywords):
        links = []
        soup = BeautifulSoup(html, "html.parser")
        base_domain = urlparse(base_url).netloc.lower()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            text = a.get_text(" ", strip=True).lower()
            if not href or href.startswith("mailto:") or href.startswith("tel:"):
                continue
            if href.startswith("#"):
                continue
            if not any(k in href.lower() or k in text for k in keywords):
                continue
            full = urljoin(base_url.rstrip("/") + "/", href)
            if urlparse(full).netloc.lower() != base_domain:
                continue
            links.append(full)
        return links
