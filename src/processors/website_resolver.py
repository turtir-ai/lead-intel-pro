#!/usr/bin/env python3
"""
Website Resolver Middleware
Fixes the "Directory Loop" problem where leads have directory sites
(OEKO-TEX, GOTS, ABIT) instead of actual company websites.

Flow:
1. Check if lead's website is a known directory domain
2. If yes, clear it and search for real website via Brave
3. Update lead with real website URL
"""

import os
import re
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse

from src.utils.logger import get_logger

logger = get_logger(__name__)


# Directory/Association domains that are NOT company websites
DIRECTORY_DOMAINS = {
    # Certification directories
    'oeko-tex.com',
    'services.oeko-tex.com',
    'global-trace-base.org',
    'gots.org',
    'bettercotton.org',
    'wrap.org',
    'higg.org',
    'bluesign.com',
    
    # Trade associations
    'abit.org.br',
    'texbrasil.com.br',
    'febratex.com.br',
    'textileexchange.org',
    'itmf.org',
    'euratex.eu',
    'cottoninc.com',
    
    # Social media
    'linkedin.com',
    'facebook.com',
    'instagram.com',
    'twitter.com',
    'youtube.com',
    
    # B2B marketplaces
    'alibaba.com',
    'made-in-china.com',
    'indiamart.com',
    'tradekey.com',
    'globalsources.com',
    'kompass.com',
    'europages.com',
    
    # Business directories
    'emis.com',
    'dnb.com',
    'hoovers.com',
    'zoominfo.com',
    'bloomberg.com',
    'reuters.com',
    
    # Reference sites
    'wikipedia.org',
    'britannica.com',
    
    # Trade publications
    'textileworld.com',
    'fibre2fashion.com',
    'just-style.com',
}


class WebsiteResolver:
    """
    Resolves directory URLs to actual company websites.
    """

    def __init__(
        self,
        brave_api_key: Optional[str] = None,
        additional_blocked_domains: Optional[Set[str]] = None,
    ):
        """
        Initialize resolver.

        Args:
            brave_api_key: Brave Search API key
            additional_blocked_domains: Extra domains to block
        """
        self.api_key = brave_api_key or os.getenv("BRAVE_API_KEY", "")
        self.blocked_domains = DIRECTORY_DOMAINS.copy()
        if additional_blocked_domains:
            self.blocked_domains.update(additional_blocked_domains)
        
        self._brave = None
        self._cache = {}

    @property
    def brave(self):
        """Lazy init Brave client."""
        if self._brave is None and self.api_key:
            from src.collectors.discovery.brave_search import BraveSearchClient
            self._brave = BraveSearchClient(self.api_key, settings={})
        return self._brave

    def is_directory_url(self, url: Optional[str]) -> bool:
        """
        Check if URL belongs to a directory/association site.

        Args:
            url: URL to check

        Returns:
            True if directory URL, False otherwise
        """
        if not url:
            return False

        try:
            domain = urlparse(str(url)).netloc.lower()
            for blocked in self.blocked_domains:
                if blocked in domain:
                    return True
            return False
        except Exception:
            return False

    def resolve_website(
        self,
        company: str,
        country: str,
        current_url: Optional[str] = None,
    ) -> Optional[str]:
        """
        Resolve actual company website.

        Args:
            company: Company name
            country: Country
            current_url: Current (possibly directory) URL

        Returns:
            Resolved website URL or None
        """
        # Check cache
        cache_key = f"{company}|{country}".lower()
        if cache_key in self._cache:
            return self._cache[cache_key]

        # If current URL is valid (not directory), return it
        if current_url and not self.is_directory_url(current_url):
            self._cache[cache_key] = current_url
            return current_url

        # Search for real website
        if not self.brave:
            logger.warning("Brave API not configured for website resolution")
            return None

        # Build search query
        query = f'"{company}" "{country}" official website -directory -association'
        
        try:
            results = self.brave.search(query, count=5)
        except Exception as e:
            logger.error(f"Website search failed for {company}: {e}")
            return None

        # Score and pick best result
        best_url = None
        best_score = 0

        for item in results:
            url = item.get("url", "")
            if not url:
                continue

            # Skip blocked domains
            if self.is_directory_url(url):
                continue

            # Score domain match
            score = self._score_match(company, url, item.get("title", ""))
            if score > best_score:
                best_score = score
                best_url = url

        if best_score >= 2:
            self._cache[cache_key] = best_url
            return best_url

        self._cache[cache_key] = None
        return None

    def _score_match(self, company: str, url: str, title: str) -> int:
        """Score how well URL matches company."""
        score = 0
        company_lower = company.lower()
        
        domain = urlparse(url).netloc.lower().replace("www.", "")
        domain_base = domain.split(".")[0]

        # Company name words in domain
        words = re.findall(r'\w{3,}', company_lower)
        for word in words:
            if word in domain_base:
                score += 2
            if word in title.lower():
                score += 1

        return score

    def resolve_lead(self, lead: Dict) -> Dict:
        """
        Resolve website for a single lead.

        Args:
            lead: Lead dict with company, country, website

        Returns:
            Updated lead with resolved website
        """
        company = str(lead.get("company", "")).strip()
        country = str(lead.get("country", "")).strip()
        current_url = str(lead.get("website", "")).strip()

        if not company:
            return lead

        # Check if current URL is a directory
        if current_url and self.is_directory_url(current_url):
            logger.debug(f"Directory URL detected for {company}: {current_url}")
            lead["directory_url_detected"] = True
            lead["original_directory_url"] = current_url
            current_url = None  # Force resolution

        # Resolve website
        resolved = self.resolve_website(company, country, current_url)

        if resolved:
            lead["website"] = resolved
            lead["website_resolved"] = True
        elif current_url and not self.is_directory_url(current_url):
            lead["website"] = current_url
        else:
            lead["website"] = ""
            lead["website_status"] = "not_found"

        return lead

    def resolve_batch(self, leads: List[Dict]) -> List[Dict]:
        """
        Resolve websites for all leads.

        Args:
            leads: List of lead dicts

        Returns:
            List of leads with resolved websites
        """
        logger.info(f"Resolving websites for {len(leads)} leads...")
        
        resolved_count = 0
        directory_count = 0

        for lead in leads:
            original = lead.get("website", "")
            if self.is_directory_url(original):
                directory_count += 1

            lead = self.resolve_lead(lead)

            if lead.get("website_resolved"):
                resolved_count += 1

        logger.info(f"Website resolution complete:")
        logger.info(f"  Directory URLs found: {directory_count}")
        logger.info(f"  Successfully resolved: {resolved_count}")

        return leads


def get_blocked_domains() -> Set[str]:
    """Return the set of blocked directory domains."""
    return DIRECTORY_DOMAINS.copy()


if __name__ == "__main__":
    # Test
    resolver = WebsiteResolver()
    
    test_leads = [
        {
            "company": "Egyptian Spinning and Weaving Co.",
            "country": "Egypt",
            "website": "https://services.oeko-tex.com/something"
        },
        {
            "company": "Vicunha Têxtil",
            "country": "Brazil",
            "website": ""
        },
        {
            "company": "Cedro Têxtil",
            "country": "Brazil",
            "website": "https://www.cedro.com.br"
        },
    ]
    
    for lead in test_leads:
        print(f"\n{lead['company']}:")
        print(f"  Original: {lead.get('website', 'none')}")
        print(f"  Is directory: {resolver.is_directory_url(lead.get('website'))}")
