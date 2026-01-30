from datetime import datetime
from urllib.parse import urlparse

from src.collectors.discovery.brave_search import BraveSearchClient
from src.processors.entity_extractor import EntityExtractor
from src.utils.evidence import record_evidence
from src.utils.http_client import HttpClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WebsiteDiscovery:
    # Reference/encyclopedia domains to reject - NOT company websites
    REFERENCE_DOMAINS = {
        'wikipedia.org', 'britannica.com', 'encyclopedia.com', 'wikidata.org',
        'merriam-webster.com', 'dictionary.com', 'thesaurus.com', 'oxfordreference.com',
        'zoominfo.com', 'bloomberg.com', 'reuters.com', 'forbes.com',
        'sciencedirect.com', 'researchgate.net', 'academia.edu', 'springer.com',
        'textileworld.com', 'fibre2fashion.com', 'just-style.com', 'apparelresources.com',
        'texdata.com', 'fashionunited.com', 'voguebusiness.com',
        'gaston.edu', 'coursera.org', 'udemy.com', 'edx.org',  # Education
        'globaltrace.org', 'global-trace-base.org',  # Certification databases
        'emis.com', 'dnb.com', 'hoovers.com', 'kompass.com',  # Business directories
        'europages.co.uk', 'europages.com',  # Listing sites
        'armut.com', 'modaknits.com', 'productmkr.com',  # Aggregator sites
        'istanbulrealestate.net', 'tekstilsayfasi.blogspot.com',  # Blog/news
        'hometextilestoday.com', 'textileworld.com',  # Trade publications
    }
    
    def __init__(
        self,
        api_key,
        settings=None,
        http_settings=None,
        policies=None,
        evidence_path="outputs/evidence/evidence_log.csv",
    ):
        self.cfg = settings or {}
        self.search = BraveSearchClient(api_key, settings=self.cfg)
        self.extractor = EntityExtractor()
        self.evidence_path = evidence_path
        self.cache = {}
        self.verify_website = bool(self.cfg.get("verify_website", False))
        self.client = None
        if self.verify_website:
            self.client = HttpClient(settings=http_settings, policies=policies)

    def discover_website(self, lead):
        if not self.cfg.get("enabled", False):
            return lead
        source_types = set(self.cfg.get("target_source_types", []) or [])
        if source_types and lead.get("source_type") not in source_types:
            return lead
        if lead.get("website") or (lead.get("websites") or []):
            return lead

        company = str(lead.get("company", "")).strip()
        if not company:
            return lead

        normalized = self.extractor.normalize_company(company)
        if not normalized:
            return lead

        if normalized in self.cache:
            website = self.cache[normalized]
            if website:
                lead["website"] = website
                lead.setdefault("websites", []).append(website)
            return lead

        country = str(lead.get("country", "")).strip()
        queries = self.cfg.get("queries") or [f"\"{company}\" {country} textile"]
        max_results = int(self.cfg.get("max_results", 5))
        disallow_domains = set(self.cfg.get("disallow_domains", []))

        best_url = ""
        best_score = 0
        for template in queries:
            query = template.format(company=company, country=country)
            results = self.search.search(query, count=max_results)
            for item in results:
                url = item.get("url", "")
                if not url:
                    continue
                domain = urlparse(url).netloc.lower()
                if any(bad in domain for bad in disallow_domains):
                    continue
                title = item.get("title", "") or ""
                desc = item.get("description", "") or ""

                score = self._score_candidate(normalized, url, title, desc)
                if score > best_score:
                    best_score = score
                    best_url = url

            if best_score >= 3:
                break

        min_score = int(self.cfg.get("min_score", 2))
        if best_score < min_score:
            best_url = ""

        if best_url and self.verify_website and self.client:
            if not self.client.get(best_url, allow_binary=True):
                best_url = ""

        if best_url:
            lead["website"] = best_url
            websites = set(lead.get("websites") or [])
            websites.add(best_url)
            lead["websites"] = sorted(websites)
            lead["website_discovered"] = True
            lead["website_discovery_score"] = best_score

            record_evidence(
                self.evidence_path,
                {
                    "source_type": "website_discovery",
                    "source_name": "Brave",
                    "url": best_url,
                    "title": company,
                    "snippet": f"Website discovery for {company}",
                    "content_hash": "",
                    "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
            )

        self.cache[normalized] = best_url
        return lead

    def _score_candidate(self, normalized_company, url, title, desc):
        score = 0
        domain = urlparse(url).netloc.lower()
        company_tokens = [t for t in normalized_company.split() if len(t) > 2]
        if not company_tokens:
            return score
        
        # REJECT reference/encyclopedia domains completely
        for ref_domain in self.REFERENCE_DOMAINS:
            if ref_domain in domain:
                return -10  # Strong negative score
        
        # REJECT if domain is a fair/directory listing page (not company website)
        listing_indicators = ['exhibitor', 'ausstellerverzeichnis', 'inscricoes', 'socios']
        for indicator in listing_indicators:
            if indicator in url.lower():
                return -5  # This is a listing page, not company site
        
        # Bonus: company name directly in domain (+5)
        domain_clean = domain.replace('www.', '').split('.')[0]
        for token in company_tokens:
            if token in domain_clean:
                score += 5  # Strong match - company name in domain
        
        # Check title/desc for company name matches
        for token in company_tokens:
            if token in title.lower():
                score += 1
            if token in desc.lower():
                score += 1
        
        # Bonus: looks like official website (short domain, .com/.br/.tr etc)
        if len(domain_clean) <= 15 and '.' in domain:
            tld = domain.split('.')[-1]
            if tld in {'com', 'br', 'tr', 'in', 'pk', 'bd', 'eg', 'pe', 'co', 'ar', 'mx', 'ec'}:
                score += 1
        
        return score
