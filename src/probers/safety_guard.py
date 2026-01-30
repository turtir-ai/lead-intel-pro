# =============================================================================
# SAFETY GUARD - V10 Ethical Endpoint Filtering
# =============================================================================
# Purpose: Ensure we only access PUBLIC data sources
#          Never access login/auth protected endpoints
#          Respect robots.txt directives
# 
# Ethical Rules:
# ✅ Public member directories
# ✅ Exhibitor lists on fair websites
# ✅ Company directories on trade associations
# ❌ Login/auth required endpoints
# ❌ Private API endpoints
# ❌ Rate-limited endpoints (respect limits)
# =============================================================================

import re
import requests
from urllib.parse import urlparse, urljoin
from typing import Optional, Tuple, List, Dict
from dataclasses import dataclass
from functools import lru_cache
import time


@dataclass
class SafetyCheckResult:
    """Result of a safety check"""
    is_safe: bool
    reason: str
    category: str  # 'allowed', 'blocked', 'robots_blocked', 'unknown'


# =============================================================================
# BLOCKED PATTERNS - Never access these endpoints
# =============================================================================

BLOCKED_PATTERNS = [
    # Authentication endpoints
    "/login", "/signin", "/sign-in", "/auth",
    "/oauth", "/sso", "/saml",
    "/token", "/session", "/jwt",
    
    # Account management
    "/account", "/profile", "/user/",
    "/my-", "/dashboard", "/admin",
    
    # Sensitive data
    "password", "secret", "api_key", "apikey",
    "private", "internal", "confidential",
    
    # Payment/Financial
    "/payment", "/billing", "/invoice",
    "/checkout", "/cart", "/order",
    
    # Personal data (GDPR)
    "personal", "gdpr", "consent",
]

# =============================================================================
# ALLOWED PATTERNS - Safe to access (public data)
# =============================================================================

ALLOWED_PATTERNS = [
    # REST API member/company endpoints
    "/api/v1/members", "/api/v2/members",
    "/api/members", "/api/companies",
    "/api/exhibitors", "/api/directory",
    "/api/suppliers", "/api/vendors",
    
    # WordPress REST API
    "/wp-json/wp/v2/posts",
    "/wp-json/wp/v2/pages",
    "/wp-json/wp/v2/categories",
    "/wp-json/",  # General WP API
    
    # Common directory patterns (Spanish/Portuguese)
    "empresas", "associados", "socios",
    "miembros", "afiliados", "expositores",
    
    # Common directory patterns (English)
    "directory", "members", "exhibitors",
    "suppliers", "partners", "companies",
    
    # Common directory patterns (German)
    "mitglieder", "aussteller", "unternehmen",
    
    # Common directory patterns (French)
    "entreprises", "membres", "exposants",
]

# =============================================================================
# RATE LIMIT CONFIGURATION
# =============================================================================

RATE_LIMITS = {
    "default": 1.0,  # 1 request per second
    "aggressive_sites": {
        "messefrankfurt.com": 2.0,
        "europages.com": 2.0,
        "wlw.de": 2.0,
    },
    "lenient_sites": {
        "bettercotton.org": 0.5,
        "tunisiatextile.com.tn": 0.5,
    }
}


class SafetyGuard:
    """
    Ethical web scraping guard.
    
    Ensures all data collection is:
    - From public sources only
    - Respecting robots.txt
    - Within rate limits
    - Not accessing personal/private data
    """
    
    def __init__(self, respect_robots: bool = True):
        self.respect_robots = respect_robots
        self._robots_cache: Dict[str, dict] = {}
        self._last_request_time: Dict[str, float] = {}
        
    def check_endpoint(self, url: str) -> SafetyCheckResult:
        """
        Check if an endpoint is safe to access.
        
        Returns SafetyCheckResult with is_safe, reason, and category.
        """
        url_lower = url.lower()
        
        # 1. Check blocked patterns first
        for pattern in BLOCKED_PATTERNS:
            if pattern in url_lower:
                return SafetyCheckResult(
                    is_safe=False,
                    reason=f"Blocked pattern detected: {pattern}",
                    category="blocked"
                )
                
        # 2. Check if matches allowed patterns
        is_explicitly_allowed = any(
            pattern in url_lower for pattern in ALLOWED_PATTERNS
        )
        
        # 3. Check robots.txt if enabled
        if self.respect_robots:
            robots_allowed = self._check_robots(url)
            if not robots_allowed:
                return SafetyCheckResult(
                    is_safe=False,
                    reason="Disallowed by robots.txt",
                    category="robots_blocked"
                )
                
        # 4. Return result
        if is_explicitly_allowed:
            return SafetyCheckResult(
                is_safe=True,
                reason="Matches allowed public data pattern",
                category="allowed"
            )
        else:
            # Unknown - be cautious
            return SafetyCheckResult(
                is_safe=False,
                reason="Unknown endpoint type - not explicitly allowed",
                category="unknown"
            )
            
    def is_safe(self, url: str) -> bool:
        """Simple boolean check for safety"""
        result = self.check_endpoint(url)
        return result.is_safe
        
    @lru_cache(maxsize=100)
    def _check_robots(self, url: str) -> bool:
        """
        Check robots.txt to see if URL is allowed.
        
        Uses caching to avoid repeated requests.
        """
        try:
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            path = parsed.path
            
            # Fetch robots.txt if not cached
            if robots_url not in self._robots_cache:
                resp = requests.get(robots_url, timeout=5)
                if resp.status_code == 200:
                    self._robots_cache[robots_url] = self._parse_robots(resp.text)
                else:
                    # No robots.txt = everything allowed
                    self._robots_cache[robots_url] = {"disallow": []}
                    
            rules = self._robots_cache.get(robots_url, {"disallow": []})
            
            # Check if path is disallowed
            for disallow_path in rules.get("disallow", []):
                if disallow_path and path.startswith(disallow_path):
                    return False
                    
            return True
            
        except Exception:
            # On error, assume allowed (fail open for usability)
            return True
            
    def _parse_robots(self, content: str) -> dict:
        """Simple robots.txt parser"""
        rules = {"disallow": [], "allow": [], "crawl_delay": None}
        current_agent = None
        
        for line in content.split('\n'):
            line = line.strip().lower()
            
            if line.startswith('user-agent:'):
                agent = line.split(':', 1)[1].strip()
                current_agent = agent
                
            elif current_agent in ['*', 'python', 'bot'] or current_agent is None:
                if line.startswith('disallow:'):
                    path = line.split(':', 1)[1].strip()
                    if path:
                        rules["disallow"].append(path)
                        
                elif line.startswith('allow:'):
                    path = line.split(':', 1)[1].strip()
                    if path:
                        rules["allow"].append(path)
                        
                elif line.startswith('crawl-delay:'):
                    try:
                        delay = float(line.split(':', 1)[1].strip())
                        rules["crawl_delay"] = delay
                    except:
                        pass
                        
        return rules
        
    def get_rate_limit(self, url: str) -> float:
        """Get appropriate rate limit for a domain"""
        parsed = urlparse(url)
        domain = parsed.netloc.replace('www.', '')
        
        # Check for domain-specific limits
        if domain in RATE_LIMITS.get("aggressive_sites", {}):
            return RATE_LIMITS["aggressive_sites"][domain]
        elif domain in RATE_LIMITS.get("lenient_sites", {}):
            return RATE_LIMITS["lenient_sites"][domain]
            
        return RATE_LIMITS["default"]
        
    def wait_for_rate_limit(self, url: str) -> None:
        """Wait if necessary to respect rate limits"""
        parsed = urlparse(url)
        domain = parsed.netloc
        
        rate_limit = self.get_rate_limit(url)
        
        if domain in self._last_request_time:
            elapsed = time.time() - self._last_request_time[domain]
            if elapsed < rate_limit:
                time.sleep(rate_limit - elapsed)
                
        self._last_request_time[domain] = time.time()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

# Global guard instance
_guard = SafetyGuard()


def is_safe_endpoint(url: str) -> bool:
    """
    Quick check if an endpoint is safe to access.
    
    Use this in APIHunter and collectors:
        from .safety_guard import is_safe_endpoint
        if is_safe_endpoint(url):
            # proceed with request
    """
    return _guard.is_safe(url)


def check_robots_txt(base_url: str, path: str = "/api/") -> bool:
    """
    Check if a path is allowed by robots.txt.
    
    Args:
        base_url: Base URL of the site (e.g., "https://example.com")
        path: Path to check (e.g., "/api/members")
        
    Returns:
        True if allowed, False if disallowed
    """
    full_url = urljoin(base_url, path)
    return _guard._check_robots(full_url)


def get_safety_report(url: str) -> SafetyCheckResult:
    """
    Get detailed safety report for a URL.
    
    Returns SafetyCheckResult with:
    - is_safe: bool
    - reason: str
    - category: 'allowed', 'blocked', 'robots_blocked', 'unknown'
    """
    return _guard.check_endpoint(url)


def wait_rate_limit(url: str) -> None:
    """Wait to respect rate limits before making a request"""
    _guard.wait_for_rate_limit(url)


# =============================================================================
# ETHICAL GUIDELINES (Documentation)
# =============================================================================

ETHICAL_GUIDELINES = """
=============================================================================
ETHICAL WEB SCRAPING GUIDELINES FOR LEAD INTEL PIPELINE
=============================================================================

✅ DO:
- Only access publicly available data (no login required)
- Respect robots.txt directives
- Use reasonable rate limits (1-2 req/sec)
- Identify yourself with a proper User-Agent
- Cache responses to avoid redundant requests
- Stop immediately if asked by site operators

❌ DON'T:
- Access endpoints that require authentication
- Bypass rate limiting or anti-bot measures
- Collect personal data (emails, phones) without consent
- Ignore robots.txt or Terms of Service
- Overload servers with too many requests
- Store or share sensitive/personal data

📊 DATA COLLECTED:
- Company names (public)
- Company websites (public)
- Business addresses (public)
- Industry/sector classification (public)
- Fair/exhibition participation (public)

🚫 DATA NOT COLLECTED:
- Personal names of employees
- Personal email addresses
- Personal phone numbers
- Social security or ID numbers
- Financial information
=============================================================================
"""


if __name__ == "__main__":
    # Test safety guard
    test_urls = [
        "https://example.com/api/v1/members",  # Should be safe
        "https://example.com/login",            # Should be blocked
        "https://example.com/api/auth/token",   # Should be blocked
        "https://example.com/empresas",         # Should be safe
        "https://example.com/admin/users",      # Should be blocked
    ]
    
    print("Safety Guard Test Results:")
    print("=" * 60)
    
    for url in test_urls:
        result = get_safety_report(url)
        status = "✅" if result.is_safe else "❌"
        print(f"{status} {url}")
        print(f"   Category: {result.category}")
        print(f"   Reason: {result.reason}")
        print()
