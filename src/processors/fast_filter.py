#!/usr/bin/env python3
"""
FastFilter - Reject-fast heuristic
Filters out obvious non-targets before expensive processing.
"""

from typing import Dict, List, Tuple
from urllib.parse import urlparse


class FastFilter:
    """Pahalı işlemlerden önce hızlı eleme."""

    # V10.4: Verified source types that bypass FastFilter entirely
    # These come from curated directories/collectors, not web scraping
    TRUSTED_SOURCE_TYPES = {
        "gots", "oekotex", "bettercotton", "bluesign",
        "fair", "known_manufacturer", "egypt_tec", "amith", "abit",
        "association_member", "directory", "oem_customer",
        "precision_search", "regional_collector",
    }

    DOMAIN_BLACKLIST = {
        ".gov", ".edu", ".mil",
        "amazon.", "alibaba.", "aliexpress.", "ebay.",
        "facebook.", "linkedin.", "twitter.", "instagram.",
        "youtube.", "wikipedia.", "medium.",
        "news", "magazine", "journal", "press",
        "blog", "wordpress", ".blogspot.",
        "directory", "portal", "listing", "yellowpages",
        "government", "ministry", "university", "school",
    }

    PATH_BLACKLIST = [
        "/members/", "/member-list/", "/directory/",
        "/listing/",
        "/category/", "/tag/", "/search/",
        "/news/", "/blog/", "/article/",
    ]

    META_BLACKLIST = [
        "news portal", "magazine", "blog",
        "directory", "listing", "member list",
    ]

    def should_reject(self, url: str, meta_description: str = "") -> Tuple[bool, str]:
        """Return (True, reason) if the lead should be rejected early."""
        if not isinstance(url, str):
            url = ""
        if not isinstance(meta_description, str):
            meta_description = ""
        url_lower = (url or "").lower()
        meta_lower = (meta_description or "").lower()

        # Domain blacklist
        for blacklisted in self.DOMAIN_BLACKLIST:
            if blacklisted in url_lower:
                return True, f"domain_blacklist:{blacklisted}"

        # Path blacklist
        try:
            path = urlparse(url_lower).path or ""
        except Exception:
            path = ""
        for pattern in self.PATH_BLACKLIST:
            if pattern in path:
                return True, f"path_blacklist:{pattern}"

        # Meta description blacklist
        for pattern in self.META_BLACKLIST:
            if pattern in meta_lower:
                return True, f"meta_blacklist:{pattern}"

        return False, ""

    def filter_batch(self, leads: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Return (passed, rejected) lists."""
        passed = []
        rejected = []
        bypassed = 0
        for lead in leads:
            # V10.4: Bypass FastFilter for trusted source types
            source_type = str(lead.get("source_type", "")).lower()
            if source_type in self.TRUSTED_SOURCE_TYPES:
                passed.append(lead)
                bypassed += 1
                continue
            
            url = lead.get("website", "") or lead.get("source_url", "")
            meta = lead.get("meta_description", "")
            should_reject, reason = self.should_reject(url, meta)
            if should_reject:
                lead["reject_reason"] = reason
                lead["reject_phase"] = "fast_filter"
                rejected.append(lead)
            else:
                passed.append(lead)
        if bypassed > 0:
            import logging
            logging.getLogger(__name__).info(f"FastFilter: {bypassed} leads bypassed (trusted source)")
        return passed, rejected
