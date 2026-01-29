"""OEKO-TEX Buying Guide / Directory Collector.

Source: https://www.oeko-tex.com/en/buying-guide
This is a public directory of OEKO-TEX certified companies.

Reverse-engineered approach:
- Profile pages are at: https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/{id}~{token}~{hash}/
- Profile pages contain: company name, address, email, contact persons
- Profile URLs can be scraped via Playwright from the Buying Guide iframe
"""
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from src.utils.cache import load_json_cache, save_json_cache
from src.utils.evidence import record_evidence
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


# ISO3 to OEKO-TEX country names and regions
COUNTRY_CONFIG = {
    "EGY": {"name": "Egypt", "region": "Africa"},
    "MAR": {"name": "Morocco", "region": "Africa"},
    "TUN": {"name": "Tunisia", "region": "Africa"},
    "DZA": {"name": "Algeria", "region": "Africa"},
    "BRA": {"name": "Brazil", "region": "Central and South America"},
    "ARG": {"name": "Argentina", "region": "Central and South America"},
    "COL": {"name": "Colombia", "region": "Central and South America"},
    "PER": {"name": "Peru", "region": "Central and South America"},
}


class OekoTexDirectory:
    """Collect leads from OEKO-TEX Buying Guide via profile page scraping."""

    def __init__(self, settings=None, evidence_path="outputs/evidence/evidence_log.csv"):
        settings = settings or {}
        self.base_url = "https://services.oeko-tex.com"
        self.profile_base = f"{self.base_url}/newoekotex/portal/for-new-website/customer_profile"
        self.timeout = settings.get("timeout", 30)
        self.max_records = int(settings.get("max_records", 500))
        self.evidence_path = evidence_path
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        # Pre-scraped profile URLs from Playwright (cached in data/raw/json/)
        self.profiles_cache_path = Path("data/raw/json/oekotex_profiles.json")

    def _load_profiles_cache(self):
        """Load pre-scraped profile URLs from cache file."""
        if self.profiles_cache_path.exists():
            try:
                with open(self.profiles_cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load profiles cache: {e}")
        return {}

    def _save_profiles_cache(self, data):
        """Save scraped profile URLs to cache file."""
        self.profiles_cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.profiles_cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _scrape_profile_page(self, profile_url: str) -> dict:
        """Scrape company details from a profile page."""
        cache_key = f"oekotex_profile:{hashlib.md5(profile_url.encode()).hexdigest()}"
        cached = load_json_cache(cache_key)
        if cached:
            return cached

        try:
            resp = self.session.get(profile_url, timeout=self.timeout)
            if resp.status_code != 200:
                return {}

            content = resp.text
            save_text_cache(profile_url, content)

            # Parse with BeautifulSoup
            soup = BeautifulSoup(content, "html.parser")
            text = soup.get_text(separator="\n", strip=True)
            lines = [l.strip() for l in text.split("\n") if l.strip()]

            result = {
                "company": "",
                "address": "",
                "email": "",
                "contacts": [],
                "profile_url": profile_url,
            }

            # Company name is usually the second line (after "Customer Profile")
            if len(lines) > 1:
                result["company"] = lines[1]

            # Extract email
            email_match = re.search(r"Email\s+([^\s\n]+@[^\s\n]+)", text)
            if email_match:
                result["email"] = email_match.group(1)

            # Alternative: find mailto links
            if not result["email"]:
                mailto = soup.find("a", href=re.compile(r"^mailto:"))
                if mailto:
                    result["email"] = mailto["href"].replace("mailto:", "")

            # Extract address (lines between company name and "Email" or "Your contact")
            address_lines = []
            collecting = False
            for line in lines:
                if collecting and ("Email" in line or "Your contact" in line):
                    break
                if collecting:
                    address_lines.append(line)
                if line == result["company"]:
                    collecting = True
            result["address"] = ", ".join(address_lines)

            # Extract contact names
            contact_matches = re.findall(r"Your contact:\s*([^\n]+)", text)
            result["contacts"] = [c.strip() for c in contact_matches if c.strip()]

            save_json_cache(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Failed to scrape profile {profile_url}: {e}")
            return {}

    def harvest(self, target_iso3=None):
        """Harvest OEKO-TEX certified companies for target countries.
        
        Uses pre-scraped profile URLs from Playwright session stored in cache,
        or falls back to scraping individual profile pages.
        """
        leads = []
        profiles_cache = self._load_profiles_cache()

        # Filter by target countries
        target_countries = set()
        for iso3 in (target_iso3 or []):
            config = COUNTRY_CONFIG.get(iso3.upper())
            if config:
                target_countries.add(config["name"])

        if not target_countries:
            # Use all configured countries if no filter
            target_countries = {cfg["name"] for cfg in COUNTRY_CONFIG.values()}

        logger.info(f"OEKO-TEX: targeting countries: {target_countries}")

        # Process cached profiles
        profiles_to_scrape = []
        for country_name in target_countries:
            country_profiles = profiles_cache.get(country_name, [])
            if country_profiles:
                logger.info(f"OEKO-TEX: found {len(country_profiles)} cached profiles for {country_name}")
                profiles_to_scrape.extend(country_profiles)
            else:
                logger.warning(f"OEKO-TEX: no cached profiles for {country_name}. Run playwright scraper first.")

        # Process each profile from cache + optional page scraping
        count = 0
        for profile in profiles_to_scrape[:self.max_records]:
            profile_url = profile.get("profile_url", "")
            
            # Use cache data first - it has company name from Playwright scraping
            company = profile.get("company", "")
            if not company:
                continue

            country = profile.get("country", "")
            location = profile.get("location", "")
            
            # Try to get additional details (email, contacts) from profile page
            details = {}
            if profile_url:
                details = self._scrape_profile_page(profile_url)
            
            # Use cached company name (more reliable)
            details["company"] = company
            
            # Parse location for city
            city = ""
            if location:
                parts = location.split("\n")
                if len(parts) > 1:
                    city = parts[-1].strip()

            snippet = f"{details['company']} | {country} | OEKO-TEX Certified"
            content_hash = hashlib.md5(snippet.encode()).hexdigest()[:16]

            record_evidence(
                self.evidence_path,
                {
                    "source_type": "oekotex",
                    "source_name": "OEKO-TEX Buying Guide",
                    "url": profile_url,
                    "title": details["company"],
                    "snippet": snippet[:400],
                    "content_hash": content_hash,
                    "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
            )

            context = (
                f"OEKO-TEX certified textile company. "
                f"Address: {details.get('address', '')}. "
                f"Contact: {', '.join(details.get('contacts', []))}."
            )

            lead = {
                "company": details["company"],
                "country": country,
                "city": city,
                "address": details.get("address", ""),
                "email": details.get("email", ""),
                "contact_name": ", ".join(details.get("contacts", []))[:100] if details.get("contacts") else "",
                "source": profile_url,
                "source_type": "oekotex",
                "source_name": "OEKO-TEX Buying Guide",
                "context": context,
                "certification": "OEKO-TEX",
            }
            leads.append(lead)
            count += 1

            if count >= self.max_records:
                break

        logger.info(f"OEKO-TEX: harvested {len(leads)} companies with details")
        return leads
