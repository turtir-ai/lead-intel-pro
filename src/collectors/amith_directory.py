"""AMITH Morocco Textile Association Directory Collector.

Source: https://www.amith.ma
AMITH (Association Marocaine des Industries du Textile et de l'Habillement)
is the official Moroccan textile industry association.
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


class AmithDirectory:
    """Collect leads from AMITH Morocco textile association."""

    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        settings = settings or {}
        self.base_url = settings.get("base_url", "https://www.amith.ma")
        self.member_paths = settings.get("member_paths", [
            "/membres",
            "/members",
            "/annuaire",
            "/directory",
            "/adherents",
        ])
        self.client = HttpClient(settings=settings, policies=policies)
        self.extractor = EntityExtractor()
        self.evidence_path = evidence_path
        self.max_pages = int(settings.get("max_pages", 20))

    def harvest(self):
        """Harvest AMITH member companies."""
        leads = []
        
        # Try different possible member directory paths
        for path in self.member_paths:
            url = urljoin(self.base_url, path)
            html = self.client.get(url)
            if html:
                page_leads = self._parse_member_page(html, url)
                leads.extend(page_leads)
                if page_leads:
                    logger.info(f"AMITH: found {len(page_leads)} members at {path}")
                    break
        
        # Also try the main page for member links
        if not leads:
            main_html = self.client.get(self.base_url)
            if main_html:
                soup = BeautifulSoup(main_html, "html.parser")
                # Find links that might lead to member directories
                for a in soup.find_all("a", href=True):
                    href = a["href"].lower()
                    text = a.get_text(" ", strip=True).lower()
                    if any(kw in href or kw in text for kw in ["membre", "member", "annuaire", "directory", "adhérent"]):
                        full_url = urljoin(self.base_url, a["href"])
                        html = self.client.get(full_url)
                        if html:
                            page_leads = self._parse_member_page(html, full_url)
                            leads.extend(page_leads)
                            if page_leads:
                                break

        logger.info(f"AMITH: harvested {len(leads)} companies total")
        return leads

    def _parse_member_page(self, html, url):
        """Parse a member directory page."""
        leads = []
        soup = BeautifulSoup(html, "html.parser")
        
        # Try different selectors for member cards
        selectors = [
            ".member-card",
            ".membre",
            ".company-card",
            ".annuaire-item",
            "article.member",
            ".list-item",
            "tr",  # table rows
        ]
        
        cards = []
        for selector in selectors:
            cards = soup.select(selector)
            if cards and len(cards) > 2:
                break
        
        for card in cards:
            name = ""
            website = ""
            email = ""
            phone = ""
            
            # Extract name from headings or strong tags
            for tag in ["h2", "h3", "h4", "h5", "strong", ".name", ".company-name", ".titre"]:
                el = card.find(tag) if not tag.startswith(".") else card.select_one(tag)
                if el:
                    name = el.get_text(strip=True)
                    if name:
                        break
            
            if not name:
                continue
            
            # Skip navigation items
            if len(name) < 3 or name.lower() in ["accueil", "home", "contact", "about"]:
                continue
            
            text = card.get_text(" ", strip=True)
            
            # Extract contact info
            emails = self.extractor.extract_emails(text)
            phones = self.extractor.extract_phones(text)
            websites = self.extractor.extract_websites(text)
            
            # Also check for links
            for a in card.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("mailto:"):
                    emails.append(href.replace("mailto:", "").split("?")[0])
                elif href.startswith("tel:"):
                    phones.append(href.replace("tel:", ""))
                elif href.startswith("http") and "amith.ma" not in href:
                    websites.append(href)
            
            website = websites[0] if websites else ""
            email = emails[0] if emails else ""
            phone = phones[0] if phones else ""
            
            snippet = f"{name} | Morocco"
            content_hash = save_text_cache(f"{url}#{name}", snippet)
            
            record_evidence(
                self.evidence_path,
                {
                    "source_type": "directory",
                    "source_name": "AMITH",
                    "url": url,
                    "title": name,
                    "snippet": snippet[:400],
                    "content_hash": content_hash,
                    "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
            )
            
            context = f"AMITH Morocco textile association member. Country: Morocco."
            leads.append({
                "company": name,
                "country": "Morocco",
                "website": website,
                "emails": [email] if email else [],
                "phones": [phone] if phone else [],
                "source": url,
                "source_type": "amith",
                "source_name": "AMITH",
                "context": context,
            })

        return leads
