#!/usr/bin/env python3
"""
DEEP VALIDATION LOOP - V7 Autonomous Hunter Module

Sonsuz Doğrulama Döngüsü (Deep Validation Loop):
1. Website Checker - HTTP 200 kontrolü
2. Keyword Scanner - Ana sayfa ve alt sayfaları tarar
3. Contact Hunter - Email ve telefon çıkarır
4. Karar Mekanizması - Tier 1/2/3 sınıflandırma

Bu modül her lead'i derinlemesine doğrular ve satışa hazır hale getirir.
"""

import os
import re
import time
import warnings
import requests
import phonenumbers
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from datetime import datetime
from bs4 import BeautifulSoup

# Suppress SSL warnings since we're using verify=False for speed
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

from src.utils.logger import get_logger
from src.utils.http_client import HttpClient

logger = get_logger(__name__)

# Thread pool for hard timeouts
_executor = ThreadPoolExecutor(max_workers=2)


# P0 Fix: Email blocklist for quality filtering
EMAIL_BLOCKLIST_PREFIXES = [
    "noreply", "no-reply", "no.reply", "donotreply",
    "example", "test", "webmaster", "admin", "info@info",
    "support@support", "mail@mail", "email@email",
]

EMAIL_BLOCKLIST_DOMAINS = [
    "example.com", "test.com", "sentry.io", "sentry-next.wixpress.com",
    "wixpress.com", "placeholder.com", "domain.com",
]


# OEM Brands for evidence detection
OEM_BRANDS = [
    "monforts", "brückner", "bruckner", "krantz", "santex", "artos",
    "babcock", "goller", "benninger", "thies", "then", "jemco",
    "dilmenler", "comet", "erbatech", "montex", "proctor", "dmc"
]

# Stenter/Finishing keywords
FINISHING_KEYWORDS = [
    # Turkish
    "ramöz", "ramoz", "stenter", "boyahane", "terbiye", "apre",
    "boya tesisi", "boyama", "germe makinesi",
    # English
    "stenter", "stentering", "tenter frame", "heat setting", "finishing",
    "dyeing", "mercerizing", "sanforizing", "calendering", "textile mill",
    # Portuguese
    "rama", "ramas", "tinturaria", "acabamento", "alvejamento",
    "beneficiamento", "estamparia",
    # Spanish
    "rama", "ramas", "tintorería", "acabado", "blanqueo", "teñido",
]

# Contact page indicators
CONTACT_PAGE_INDICATORS = [
    "contact", "contato", "contacto", "iletisim", "iletişim",
    "kontakt", "about", "hakkimizda", "hakkımızda", "sobre",
]

# Email regex
EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

# Phone regex (international format)
PHONE_REGEX = re.compile(r'[\+\d\(\)\s\-]{8,20}')


class DeepValidator:
    """
    Deep validation loop for leads.
    
    Validates each lead through:
    1. Website accessibility check
    2. Keyword scanning (finishing/stenter signals)
    3. Contact extraction (email, phone)
    4. Tier classification
    """
    
    def __init__(
        self,
        http_client: Optional[HttpClient] = None,
        max_pages_per_site: int = 5,
        timeout: int = 10,
        max_lead_seconds: int = 60,
    ):
        self.http = http_client or HttpClient()
        self.max_pages = max_pages_per_site
        self.timeout = timeout
        self.max_lead_seconds = max_lead_seconds
        
        # Stats
        self.stats = {
            "total_validated": 0,
            "websites_accessible": 0,
            "keywords_found": 0,
            "emails_found": 0,
            "phones_found": 0,
            "tier_1": 0,
            "tier_2": 0,
            "tier_3": 0,
            "fail_reasons": {},  # P0: Track why websites fail
        }
    
    def validate_lead(self, lead: Dict) -> Dict:
        """
        Run deep validation on a single lead.
        
        Returns enriched lead with validation results.
        """
        company = lead.get("company", "")
        website = lead.get("website", "")
        
        # Guard against NaN values from pandas
        if not isinstance(website, str):
            website = ""
        if not isinstance(company, str):
            company = str(company) if company else ""
        
        self.stats["total_validated"] += 1
        start_ts = time.monotonic()
        
        result = {
            "validation_status": "pending",
            "website_accessible": False,
            "has_finishing_keywords": False,
            "finishing_signals": [],
            "oem_signals": [],
            "emails_extracted": [],
            "phones_extracted": [],
            "pages_scanned": 0,
            "tier": 3,  # Default to lowest
            "validated_at": datetime.now().isoformat(),
            "fail_reason": "",  # P0: Track why validation failed
        }
        
        if not website:
            result["validation_status"] = "no_website"
            lead.update(result)
            return lead
        
        # Step 1: Check website accessibility (P0: now returns fail_reason)
        is_accessible, homepage_html, fail_reason = self._check_website(website)
        result["website_accessible"] = is_accessible
        result["fail_reason"] = fail_reason
        
        if not is_accessible:
            result["validation_status"] = f"website_inaccessible:{fail_reason}"
            # P0: Track fail reasons in stats
            if fail_reason:
                self.stats["fail_reasons"][fail_reason] = self.stats["fail_reasons"].get(fail_reason, 0) + 1
            lead.update(result)
            return lead
        
        self.stats["websites_accessible"] += 1
        
        # Step 2: Scan homepage for keywords
        all_text = homepage_html
        pages_scanned = 1
        
        # Step 3: Find and scan additional pages
        additional_pages = self._find_key_pages(website, homepage_html)
        for page_url in additional_pages[:self.max_pages - 1]:
            if time.monotonic() - start_ts > self.max_lead_seconds:
                result["validation_status"] = "lead_timeout"
                result["fail_reason"] = "lead_timeout"
                lead.update(result)
                return lead
            page_html = self._fetch_page(page_url)
            if page_html:
                all_text += " " + page_html
                pages_scanned += 1
        
        result["pages_scanned"] = pages_scanned
        
        # Step 4: Extract keywords
        finishing_signals = self._extract_finishing_signals(all_text)
        oem_signals = self._extract_oem_signals(all_text)
        
        result["finishing_signals"] = finishing_signals
        result["oem_signals"] = oem_signals
        result["has_finishing_keywords"] = len(finishing_signals) > 0 or len(oem_signals) > 0
        
        if result["has_finishing_keywords"]:
            self.stats["keywords_found"] += 1
        
        # Step 5: Extract contacts
        emails = self._extract_emails(all_text)
        phones = self._extract_phones(all_text)
        
        result["emails_extracted"] = emails[:5]  # Max 5
        result["phones_extracted"] = phones[:3]  # Max 3
        
        if emails:
            self.stats["emails_found"] += 1
        if phones:
            self.stats["phones_found"] += 1
        
        # Step 6: Determine Tier
        tier = self._calculate_tier(result)
        result["tier"] = tier
        result["validation_status"] = "validated"
        
        if tier == 1:
            self.stats["tier_1"] += 1
        elif tier == 2:
            self.stats["tier_2"] += 1
        else:
            self.stats["tier_3"] += 1
        
        lead.update(result)
        return lead
    
    def _check_website(self, url: str) -> Tuple[bool, str, str]:
        """
        Check if website is accessible and return HTML.
        P0 Fix: SSL fallback + reason logging.
        
        Returns:
            (is_accessible, html_content, fail_reason)
        """
        # Guard against NaN values
        if not url or not isinstance(url, str):
            return False, "", "invalid_url"
        
        original_url = url
        
        # Ensure URL has scheme
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        
        # Try HTTPS first, then HTTP fallback
        for attempt_url in [url, url.replace("https://", "http://")]:
            try:
                response = requests.get(
                    attempt_url,
                    timeout=(3, 10),  # Aggressive: 3s connect, 10s read (was 5, self.timeout)
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.5",
                    },
                    allow_redirects=True,
                    verify=False,  # Skip SSL for speed - we're scanning content not transacting
                )
                
                if response.status_code == 200:
                    # Check for CloudFlare challenge
                    if "cf-ray" in response.text.lower() and len(response.text) < 2000:
                        self.stats.setdefault("fail_reasons", {})
                        self.stats["fail_reasons"]["cloudflare"] = self.stats["fail_reasons"].get("cloudflare", 0) + 1
                        continue  # Try HTTP fallback
                    return True, response.text, ""
                elif response.status_code == 403:
                    self.stats.setdefault("fail_reasons", {})
                    self.stats["fail_reasons"]["403_forbidden"] = self.stats["fail_reasons"].get("403_forbidden", 0) + 1
                elif response.status_code == 404:
                    return False, "", "404_not_found"
                    
            except requests.exceptions.SSLError as e:
                logger.debug(f"SSL error for {attempt_url}: {e}")
                self.stats.setdefault("fail_reasons", {})
                self.stats["fail_reasons"]["ssl_error"] = self.stats["fail_reasons"].get("ssl_error", 0) + 1
                continue  # Try HTTP fallback
                
            except requests.exceptions.Timeout:
                self.stats.setdefault("fail_reasons", {})
                self.stats["fail_reasons"]["timeout"] = self.stats["fail_reasons"].get("timeout", 0) + 1
                return False, "", "timeout"
                
            except requests.exceptions.ConnectionError as e:
                error_str = str(e).lower()
                if "reset" in error_str:
                    reason = "connection_reset"
                elif "refused" in error_str:
                    reason = "connection_refused"
                else:
                    reason = "connection_error"
                self.stats.setdefault("fail_reasons", {})
                self.stats["fail_reasons"][reason] = self.stats["fail_reasons"].get(reason, 0) + 1
                return False, "", reason
                
            except Exception as e:
                logger.debug(f"Website check failed for {attempt_url}: {e}")
                return False, "", "unknown_error"
        
        return False, "", "all_attempts_failed"
    
    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a single page with strict timeout."""
        try:
            response = requests.get(
                url,
                timeout=(3, 8),  # Aggressive: 3s connect, 8s read (was 5, 10)
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                },
                verify=False,  # Skip SSL verification for speed - we're just scanning content
                allow_redirects=True,
            )
            if response.status_code == 200:
                # Limit content size to prevent memory issues
                return response.text[:500000]  # Max 500KB
        except requests.exceptions.Timeout:
            logger.debug(f"Page fetch timeout: {url}")
        except requests.exceptions.SSLError:
            logger.debug(f"Page fetch SSL error: {url}")
        except Exception as e:
            logger.debug(f"Page fetch error: {url} - {e}")
        return None
    
    def _find_key_pages(self, base_url: str, html: str) -> List[str]:
        """Find contact, about, and production pages."""
        key_pages = []
        
        try:
            soup = BeautifulSoup(html, "html.parser")
            links = soup.find_all("a", href=True)
            
            for link in links:
                href = link.get("href", "")
                text = link.get_text().lower().strip()
                
                # Check for contact/about page indicators
                is_key_page = any(ind in text or ind in href.lower() 
                                 for ind in CONTACT_PAGE_INDICATORS)
                
                if is_key_page:
                    # Resolve relative URLs
                    full_url = urljoin(base_url, href)
                    if full_url not in key_pages:
                        key_pages.append(full_url)
            
        except Exception as e:
            logger.debug(f"Error finding key pages: {e}")
        
        return key_pages[:5]
    
    def _extract_finishing_signals(self, text: str) -> List[str]:
        """Extract finishing/stenter keywords from text."""
        if not isinstance(text, str):
            return []
        signals = []
        text_lower = text.lower()
        
        for keyword in FINISHING_KEYWORDS:
            if keyword in text_lower:
                if keyword not in signals:
                    signals.append(keyword)
        
        return signals
    
    def _extract_oem_signals(self, text: str) -> List[str]:
        """Extract OEM brand mentions from text."""
        if not isinstance(text, str):
            return []
        signals = []
        text_lower = text.lower()
        
        for brand in OEM_BRANDS:
            if brand in text_lower:
                if brand not in signals:
                    signals.append(brand)
        
        return signals
    
    def _extract_emails(self, text: str) -> List[str]:
        """
        Extract email addresses from text.
        P0 Fix: Enhanced filtering with blocklist.
        """
        if not isinstance(text, str):
            return []
        found = EMAIL_REGEX.findall(text)
        
        # Filter out common false positives
        filtered = []
        for email in found:
            email_lower = email.lower()
            
            # Skip images, js files, CSS, etc.
            if any(ext in email_lower for ext in [".png", ".jpg", ".gif", ".css", ".js", ".svg", ".ico", ".woff"]):
                continue
            
            # P0: Skip blocklisted domains
            if any(domain in email_lower for domain in EMAIL_BLOCKLIST_DOMAINS):
                continue
            
            # P0: Skip blocklisted prefixes
            local_part = email_lower.split("@")[0]
            if any(local_part.startswith(prefix) for prefix in EMAIL_BLOCKLIST_PREFIXES):
                continue
            
            # Skip generic/useless emails
            if local_part in ["info", "contact", "sales", "hello", "office"]:
                # These are still useful but lower priority - keep them
                pass
            
            if email_lower not in [e.lower() for e in filtered]:
                filtered.append(email)
        
        return filtered
    
    def _extract_phones(self, text: str) -> List[str]:
        """
        Extract phone numbers from text.
        P0 Fix: Use phonenumbers library for accurate extraction.
        """
        if not isinstance(text, str):
            return []
        phones = []
        
        # Try phonenumbers library first (much more accurate)
        try:
            for match in phonenumbers.PhoneNumberMatcher(text, None):
                phone_str = phonenumbers.format_number(
                    match.number, 
                    phonenumbers.PhoneNumberFormat.E164
                )
                if phone_str not in phones:
                    phones.append(phone_str)
        except Exception as e:
            logger.debug(f"phonenumbers parsing error: {e}")
            # Fallback to regex if library fails
            found = PHONE_REGEX.findall(text)
            for phone in found:
                digits = re.sub(r'\D', '', phone)
                # Must have 10-15 digits and not be a common false positive
                if 10 <= len(digits) <= 15:
                    # Skip SVG viewBox patterns (0 0 XXX XXX)
                    if digits.startswith("00") and len(set(digits[:4])) <= 2:
                        continue
                    # Skip common false positives
                    if digits in ["10000000", "12345678", "00000000", "1234567890"]:
                        continue
                    cleaned = phone.strip()
                    if cleaned not in phones:
                        phones.append(cleaned)
        
        return phones
    
    def _calculate_tier(self, result: Dict) -> int:
        """
        Calculate lead tier based on validation results.
        
        Tier 1: Website + Finishing Keywords + Email = SALES READY
        Tier 2: Website + (Keywords OR Email) = PROMISING
        Tier 3: Just website or nothing = NEEDS RESEARCH
        """
        has_website = result["website_accessible"]
        has_keywords = result["has_finishing_keywords"]
        has_email = len(result["emails_extracted"]) > 0
        has_oem = len(result["oem_signals"]) > 0
        
        # Tier 1: Full validation
        if has_website and has_keywords and has_email:
            return 1
        
        # Tier 1 also if OEM evidence + website
        if has_website and has_oem:
            return 1
        
        # Tier 2: Partial validation
        if has_website and (has_keywords or has_email):
            return 2
        
        # Tier 3: Minimal or no validation
        return 3
    
    def _validate_lead_with_timeout(self, lead: Dict, timeout_seconds: int = 30) -> Dict:
        """Validate lead with hard thread-based timeout."""
        future = _executor.submit(self.validate_lead, lead)
        try:
            return future.result(timeout=timeout_seconds)
        except FuturesTimeoutError:
            logger.warning(f"HARD TIMEOUT: {lead.get('company', 'Unknown')[:30]} after {timeout_seconds}s")
            lead["validation_status"] = "hard_timeout"
            lead["fail_reason"] = f"hard_timeout_{timeout_seconds}s"
            lead["tier"] = 3
            lead["website_accessible"] = False
            return lead
        except Exception as e:
            logger.warning(f"Thread error: {lead.get('company', 'Unknown')[:30]} - {e}")
            lead["validation_status"] = "thread_error"
            lead["fail_reason"] = str(e)[:100]
            lead["tier"] = 3
            return lead
    
    def validate_batch(
        self,
        leads: List[Dict],
        progress_callback: Optional[callable] = None,
        checkpoint_every: int = 25,
        checkpoint_dir: Optional[str] = None,
        hard_timeout: int = 30,
    ) -> List[Dict]:
        """
        Validate a batch of leads with checkpoint support and HARD timeout.
        
        Args:
            leads: List of leads to validate
            progress_callback: Optional callback for progress updates
            checkpoint_every: Save checkpoint every N leads (default 25)
            checkpoint_dir: Directory for checkpoint files
            hard_timeout: Hard timeout per lead in seconds (default 30)
        
        Returns list of validated leads.
        """
        import pandas as pd
        
        total_leads = len(leads)
        logger.info(f"Deep validating {total_leads} leads (hard timeout: {hard_timeout}s, checkpoint every {checkpoint_every})...")
        
        # Setup checkpoint directory
        if checkpoint_dir is None:
            checkpoint_dir = "data/staging"
        os.makedirs(checkpoint_dir, exist_ok=True)
        checkpoint_file = os.path.join(checkpoint_dir, "validation_checkpoint.csv")
        
        validated = []
        batch_start_time = time.monotonic()
        timeouts_count = 0
        
        for i, lead in enumerate(leads):
            lead_start = time.monotonic()
            
            # Use thread-based hard timeout
            validated_lead = self._validate_lead_with_timeout(lead, timeout_seconds=hard_timeout)
            
            lead_elapsed = time.monotonic() - lead_start
            validated_lead["validation_time_seconds"] = round(lead_elapsed, 2)
            
            if validated_lead.get("validation_status") == "hard_timeout":
                timeouts_count += 1
            
            validated.append(validated_lead)
            
            # Progress logging - every 5 leads for better visibility
            if progress_callback:
                progress_callback(i + 1, total_leads)
            elif (i + 1) % 5 == 0 or (i + 1) == total_leads:
                elapsed_total = time.monotonic() - batch_start_time
                rate = (i + 1) / elapsed_total if elapsed_total > 0 else 0
                eta = (total_leads - i - 1) / rate if rate > 0 else 0
                logger.info(f"Validation: {i + 1}/{total_leads} | "
                           f"Rate: {rate:.1f}/s | ETA: {eta/60:.1f}min | "
                           f"T1: {self.stats.get('tier_1', 0)} | TO: {timeouts_count}")
            
            # Checkpoint save
            if (i + 1) % checkpoint_every == 0:
                try:
                    df_checkpoint = pd.DataFrame(validated)
                    df_checkpoint.to_csv(checkpoint_file, index=False)
                    logger.info(f"💾 Checkpoint saved: {i + 1} leads -> {checkpoint_file}")
                except Exception as e:
                    logger.warning(f"Checkpoint save failed: {e}")
            
            # Polite delay (reduced from 0.5 to 0.3 for faster processing)
            time.sleep(0.3)
        
        # Final checkpoint
        try:
            df_final = pd.DataFrame(validated)
            df_final.to_csv(checkpoint_file, index=False)
            logger.info(f"✅ Final checkpoint saved: {len(validated)} leads")
        except Exception as e:
            logger.warning(f"Final checkpoint failed: {e}")
        
        logger.info(f"Deep validation complete. Stats: {self.stats}")
        return validated
    
    def get_stats(self) -> Dict:
        """Return validation statistics."""
        return self.stats.copy()


class TierExporter:
    """Export validated leads by tier."""
    
    @staticmethod
    def export_by_tier(
        leads: List[Dict],
        output_dir: str,
        timestamp: Optional[str] = None,
    ) -> Dict[int, str]:
        """
        Export leads separated by tier.
        
        Returns dict mapping tier -> filepath.
        """
        import pandas as pd
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
        
        tier_files = {}
        
        for tier in [1, 2, 3]:
            tier_leads = [l for l in leads if l.get("tier") == tier]
            if tier_leads:
                df = pd.DataFrame(tier_leads)
                
                # Select key columns
                key_cols = [
                    "company", "country", "website", "emails_extracted",
                    "phones_extracted", "finishing_signals", "oem_signals",
                    "tier", "validation_status",
                ]
                available_cols = [c for c in key_cols if c in df.columns]
                df = df[available_cols]
                
                filename = f"tier_{tier}_leads_{timestamp}.csv"
                filepath = os.path.join(output_dir, filename)
                df.to_csv(filepath, index=False)
                tier_files[tier] = filepath
                
                logger.info(f"Tier {tier}: Exported {len(tier_leads)} leads to {filepath}")
        
        return tier_files
