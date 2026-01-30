from urllib.parse import urlparse

from src.processors.entity_extractor import EntityExtractor
from src.processors.website_discovery import WebsiteDiscovery
from src.processors.contact_enricher import ContactEnricher
from src.utils.logger import get_logger

logger = get_logger(__name__)

# GPT Fix #1: Free email domains to exclude from website inference
FREE_EMAIL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'live.com',
    'aol.com', 'icloud.com', 'mail.com', 'protonmail.com', 'zoho.com',
    'yandex.com', 'gmx.com', 'gmx.de', 'web.de', 'mail.ru', 'qq.com',
    '163.com', '126.com', 'sina.com', 'msn.com', 'me.com', 'mac.com',
    'googlemail.com', 'pm.me', 'tutanota.com', 'fastmail.com'
}

# GPT Fix #2: Social/marketplace domains to exclude from source_url -> website
EXCLUDE_SOURCE_DOMAINS = {
    'linkedin.com', 'facebook.com', 'twitter.com', 'instagram.com',
    'youtube.com', 'tiktok.com', 'pinterest.com', 'reddit.com',
    'alibaba.com', 'aliexpress.com', 'amazon.com', 'ebay.com',
    'made-in-china.com', 'indiamart.com', 'thomasnet.com',
    'europages.com', 'kompass.com', 'dnb.com', 'zoominfo.com',
    'bloomberg.com', 'reuters.com', 'wikipedia.org', 'britannica.com'
}


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

        # GPT Fix #1: Email → Website inference
        lead = self._infer_website_from_email(lead)
        
        # GPT Fix #2: source_url → website transfer
        lead = self._transfer_source_url_to_website(lead)

        return lead

    def _infer_website_from_email(self, lead):
        """GPT Fix #1: If website is empty but email is corporate, infer website from email domain."""
        if lead.get("website") and str(lead.get("website")).lower() not in {"nan", "none", "", "[]"}:
            return lead  # Already has website
        
        emails = lead.get("emails") or []
        if isinstance(emails, str):
            # Parse string representation of list
            if emails.startswith("["):
                try:
                    import ast
                    emails = ast.literal_eval(emails)
                except:
                    emails = [emails]
            else:
                emails = [emails]
        
        for email in emails:
            if not email or not isinstance(email, str) or "@" not in email:
                continue
            domain = email.split("@")[-1].lower().strip()
            if domain in FREE_EMAIL_DOMAINS:
                continue
            # Valid corporate domain found
            inferred_website = f"https://{domain}"
            lead["website"] = inferred_website
            lead["website_source"] = "email_inference"
            logger.debug(f"Inferred website {inferred_website} from email {email}")
            break
        
        return lead

    def _transfer_source_url_to_website(self, lead):
        """GPT Fix #2: If website is empty and source_url is company site, use it as website."""
        if lead.get("website") and str(lead.get("website")).lower() not in {"nan", "none", "", "[]"}:
            return lead  # Already has website
        
        source_url = lead.get("source") or lead.get("source_url") or ""
        if not source_url or not isinstance(source_url, str):
            return lead
        if source_url.lower() in {"nan", "none", ""}:
            return lead
        
        # Extract domain from source_url
        try:
            parsed = urlparse(source_url)
            domain = parsed.netloc.lower()
            # Remove www. prefix for comparison
            domain_clean = domain.replace("www.", "")
            
            # Check if it's a social/marketplace domain
            if any(excl in domain_clean for excl in EXCLUDE_SOURCE_DOMAINS):
                return lead
            
            # Valid source - use as website
            base_url = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme else f"https://{parsed.netloc}"
            lead["website"] = base_url
            lead["website_source"] = "source_url_transfer"
            logger.debug(f"Transferred source_url to website: {base_url}")
        except Exception:
            pass
        
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
