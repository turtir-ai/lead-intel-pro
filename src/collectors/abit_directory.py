"""ABIT - Associação Brasileira da Indústria Têxtil Collector.

Source: https://www.abit.org.br
ABIT is the Brazilian Textile and Apparel Industry Association.
Major source for Brazilian textile companies.
"""
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.processors.entity_extractor import EntityExtractor
from src.utils.evidence import record_evidence
from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


class AbitDirectory:
    """Collect leads from ABIT Brazil textile association."""

    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        settings = settings or {}
        self.base_url = settings.get("base_url", "https://www.abit.org.br")
        self.member_paths = settings.get("member_paths", [
            "/associados",
            "/associados/lista",
            "/empresas",
            "/members",
            "/sobre/associados",
        ])
        self.client = HttpClient(settings=settings, policies=policies)
        self.extractor = EntityExtractor()
        self.evidence_path = evidence_path
        self.max_pages = int(settings.get("max_pages", 30))

    def harvest(self):
        """Harvest ABIT member companies."""
        leads = []
        
        # Try different possible member directory paths
        for path in self.member_paths:
            url = urljoin(self.base_url, path)
            html = self.client.get(url)
            if html:
                page_leads = self._parse_member_page(html, url)
                leads.extend(page_leads)
                if page_leads:
                    logger.info(f"ABIT: found {len(page_leads)} members at {path}")
                    # Try pagination
                    for page in range(2, self.max_pages + 1):
                        page_url = f"{url}?page={page}"
                        page_html = self.client.get(page_url)
                        if not page_html:
                            break
                        more_leads = self._parse_member_page(page_html, page_url)
                        if not more_leads:
                            break
                        leads.extend(more_leads)
                    break
        
        # Deduplicate by company name
        seen = set()
        unique_leads = []
        for lead in leads:
            key = lead["company"].lower().strip()
            if key not in seen:
                seen.add(key)
                unique_leads.append(lead)

        logger.info(f"ABIT: harvested {len(unique_leads)} companies total")
        return unique_leads

    def _parse_member_page(self, html, url):
        """Parse a member directory page."""
        leads = []
        soup = BeautifulSoup(html, "html.parser")
        
        # Try different selectors for member cards
        selectors = [
            ".associado",
            ".member-card",
            ".empresa",
            ".company-card",
            "article.associado",
            ".list-item",
            ".card",
        ]
        
        cards = []
        for selector in selectors:
            cards = soup.select(selector)
            if cards and len(cards) > 2:
                break
        
        # If no cards found, try to find a list structure
        if not cards:
            # Look for lists with company names
            for ul in soup.find_all(["ul", "ol"]):
                items = ul.find_all("li")
                if len(items) > 5:
                    cards = items
                    break
        
        for card in cards:
            name = ""
            website = ""
            
            # Extract name
            for tag in ["h2", "h3", "h4", "h5", "strong", "a", ".name", ".empresa-nome"]:
                el = card.find(tag) if not tag.startswith(".") else card.select_one(tag)
                if el:
                    candidate = el.get_text(strip=True)
                    # Skip if it looks like a navigation link
                    if candidate and len(candidate) > 3 and candidate.lower() not in ["ver mais", "saiba mais", "detalhes"]:
                        name = candidate
                        break
            
            if not name:
                # Try the card's direct text
                text = card.get_text(" ", strip=True)
                if text and len(text) > 3 and len(text) < 100:
                    name = text.split("|")[0].strip()
            
            if not name or len(name) < 3:
                continue
            
            # Skip navigation items
            skip_words = ["menu", "home", "contato", "sobre", "notícias", "eventos"]
            if any(w in name.lower() for w in skip_words):
                continue
            
            text = card.get_text(" ", strip=True)
            
            # Extract website
            websites = self.extractor.extract_websites(text)
            for a in card.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("http") and "abit.org.br" not in href:
                    websites.append(href)
            
            website = websites[0] if websites else ""
            
            snippet = f"{name} | Brazil"
            content_hash = save_text_cache(f"{url}#{name}", snippet)
            
            record_evidence(
                self.evidence_path,
                {
                    "source_type": "directory",
                    "source_name": "ABIT",
                    "url": url,
                    "title": name,
                    "snippet": snippet[:400],
                    "content_hash": content_hash,
                    "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
            )
            
            context = f"ABIT Brazil textile association member. Country: Brazil."
            leads.append({
                "company": name,
                "country": "Brazil",
                "website": website,
                "source": url,
                "source_type": "abit",
                "source_name": "ABIT",
                "context": context,
            })

        return leads
