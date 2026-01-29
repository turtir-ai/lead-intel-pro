"""bluesign System Partners List Collector.

Source: https://www.bluesign.com/en/business/system-partners
The list is available as PDF and on the website.
"""
from datetime import datetime

from bs4 import BeautifulSoup

from src.processors.entity_extractor import EntityExtractor
from src.utils.evidence import record_evidence
from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


# ISO3 to country name mapping
COUNTRY_NAMES = {
    "MAR": "Morocco",
    "DZA": "Algeria",
    "TUN": "Tunisia",
    "EGY": "Egypt",
    "BRA": "Brazil",
    "ARG": "Argentina",
    "COL": "Colombia",
    "PER": "Peru",
}


class BluesignPartners:
    """Collect leads from bluesign System Partners."""

    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        settings = settings or {}
        self.base_url = settings.get(
            "base_url",
            "https://www.bluesign.com/en/business/system-partners",
        )
        self.client = HttpClient(settings=settings, policies=policies)
        self.extractor = EntityExtractor()
        self.evidence_path = evidence_path

    def harvest(self, target_iso3=None):
        """Harvest bluesign system partners."""
        leads = []
        
        # Convert ISO3 codes to country names for filtering
        target_countries = set()
        for iso3 in (target_iso3 or []):
            name = COUNTRY_NAMES.get(iso3.upper())
            if name:
                target_countries.add(name.lower())

        html = self.client.get(self.base_url)
        if not html:
            logger.warning("Could not fetch bluesign partners page")
            return leads

        soup = BeautifulSoup(html, "html.parser")
        
        # Look for partner cards or table rows
        cards = soup.select(".partner-card, .partner-item, tr.partner, .card")
        if not cards:
            # Try finding by structure - look for company listings
            cards = soup.select("article, .list-item, li.partner")
        
        for card in cards:
            name = ""
            country = ""
            website = ""
            
            # Extract name
            name_el = card.find(["h3", "h4", "h5", "strong", ".name", ".company-name"])
            if name_el:
                name = name_el.get_text(strip=True)
            
            if not name:
                # Try first strong or bold text
                strong = card.find("strong")
                if strong:
                    name = strong.get_text(strip=True)
            
            if not name:
                continue
            
            # Extract country
            text = card.get_text(" ", strip=True)
            for iso3, country_name in COUNTRY_NAMES.items():
                if country_name.lower() in text.lower():
                    country = country_name
                    break
            
            # Check if this is a target country (if filtering)
            if target_countries and country.lower() not in target_countries:
                continue
            
            # Extract website
            for a in card.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("http") and "bluesign.com" not in href:
                    website = href
                    break
            
            # Also try to extract from text
            if not website:
                websites = self.extractor.extract_websites(text)
                if websites:
                    website = websites[0]
            
            snippet = f"{name} | {country}"
            content_hash = save_text_cache(f"{self.base_url}#{name}", snippet)
            
            record_evidence(
                self.evidence_path,
                {
                    "source_type": "directory",
                    "source_name": "bluesign",
                    "url": self.base_url,
                    "title": name,
                    "snippet": snippet[:400],
                    "content_hash": content_hash,
                    "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
            )
            
            context = f"bluesign System Partner. Country: {country}."
            leads.append({
                "company": name,
                "country": country,
                "website": website,
                "source": self.base_url,
                "source_type": "bluesign",
                "source_name": "bluesign",
                "context": context,
            })

        # Also try to find and parse PDF link if present
        pdf_link = soup.find("a", href=lambda h: h and ".pdf" in h.lower())
        if pdf_link:
            pdf_url = pdf_link["href"]
            if not pdf_url.startswith("http"):
                pdf_url = f"https://www.bluesign.com{pdf_url}"
            logger.info(f"Found bluesign PDF: {pdf_url}")
            # PDF processing would be handled by pdf_processor

        logger.info(f"bluesign: harvested {len(leads)} partners")
        return leads
