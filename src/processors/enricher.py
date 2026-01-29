from urllib.parse import urlparse

from src.processors.entity_extractor import EntityExtractor
from src.processors.website_discovery import WebsiteDiscovery
from src.processors.contact_enricher import ContactEnricher
from src.utils.logger import get_logger

logger = get_logger(__name__)


class Enricher:
    def __init__(self, targets_config=None, settings=None, sources=None, policies=None):
        self.targets = targets_config or {}
        self.settings = settings or {}
        self.sources = sources or {}
        self.policies = policies or {}
        self.extractor = EntityExtractor()
        enrichment_cfg = self.settings.get("enrichment", {})
        website_cfg = enrichment_cfg.get("website_discovery", {})
        contact_cfg = enrichment_cfg.get("contact", {})

        self.website_discovery = None
        if website_cfg.get("enabled"):
            api_key = (self.settings.get("api_keys") or {}).get("brave")
            self.website_discovery = WebsiteDiscovery(
                api_key,
                settings=website_cfg,
                http_settings=self.settings,
                policies=self.policies,
            )

        self.contact_enricher = None
        if contact_cfg.get("enabled"):
            self.contact_enricher = ContactEnricher(
                settings=self.settings,
                policies=self.policies,
                contact_cfg=contact_cfg,
            )

    def enrich(self, leads):
        enriched = []
        for lead in leads:
            enriched.append(self.enrich_one(lead))
        return enriched

    def enrich_one(self, lead):
        context = lead.get("context", "")
        # Handle NaN/float/None values from CSV parsing
        if context is None or (isinstance(context, float) and str(context) == "nan"):
            context = ""
        context = str(context) if context else ""
        lead["emails"] = self.extractor.extract_emails(context)
        lead["phones"] = self.extractor.extract_phones(context)
        websites = set(self.extractor.extract_websites(context))
        if lead.get("website") and str(lead.get("website")).lower() not in {"nan", "none", "null"}:
            websites.add(str(lead["website"]))
        websites = {str(site) for site in websites if site and str(site) != "nan"}
        lead["websites"] = sorted(websites)
        if lead["websites"] and not lead.get("website"):
            lead["website"] = lead["websites"][0]

        lead["normalized_company"] = self.extractor.normalize_company(lead.get("company", ""))

        source_url = lead.get("source")
        # Handle NaN/float values
        if source_url and not isinstance(source_url, str):
            source_url = str(source_url) if str(source_url) not in {"nan", "None", ""} else ""
        lead["source_domain"] = self._domain(source_url) if source_url else ""
        lead["country_mentions"] = self._match_countries(context)

        if self.website_discovery:
            lead = self.website_discovery.discover_website(lead)

        if self.contact_enricher:
            lead = self.contact_enricher.enrich(lead)

        return lead

    def _domain(self, url):
        if not url or not isinstance(url, str):
            return ""
        if str(url).lower() in {"nan", "none", ""}:
            return ""
        parsed = urlparse(str(url))
        return parsed.netloc.lower()

    def _match_countries(self, text):
        text_l = (text or "").lower()
        hits = []
        for _, data in self.targets.get("target_regions", {}).items():
            for label in data.get("labels", []):
                if label.lower() in text_l:
                    hits.append(label)
        return sorted(set(hits))
