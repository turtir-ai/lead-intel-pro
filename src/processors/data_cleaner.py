"""
Data Cleaner Module - Phase 1: Data Quality Foundation
Filters noise and validates entity data for B2B lead pipeline
"""

import re
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Filters noise and validates entity data
    Enhanced with stricter noise filters based on GPT audit recommendations.
    """
    
    # Noise patterns - companies that aren't real businesses
    NOISE_KEYWORDS = [
        "event", "news", "textile", "dyeing", "finishing", 
        "machine", "manufacturer", "limited", "review", "yarn",
        "summit", "conference", "expo", "fair", "exhibition",
        "association", "council", "federation", "chamber"
    ]
    
    # GPT Audit: Non-customer entity types to filter
    # These are NOT stenter customers - they don't have finishing lines
    NON_CUSTOMER_INDICATORS = [
        # Labels/Packaging (not fabric finishing)
        "labels", "label", "labeling", "packaging", "etiket",
        # Plastic/Fiber (different machinery)
        "plastic", "plastics", "fiber industry", "synthetic fiber",
        # Garment-only (no dyehouse, just cutting/sewing)
        # Note: "garment" alone isn't filtered if combined with "dyeing"
        # Software/Consulting
        "software", "consulting", "consultant", "erp", "mes",
        # Machinery suppliers (competitors, not customers)
        "machinery supplier", "machine supplier", "spare parts",
        "machinery dealer", "equipment dealer", "parts supplier",
        # Organizations (not businesses) — V10.5: added association, federation, chamber
        "university", "institute", "research center", "academy",
        "government", "ministry", "directorate", "council",
        "association", "federation", "chamber of commerce", "chamber",
        "board of trade", "trade body", "trade union",
        # V10.5: Media entities
        "magazine", "journal", "newsletter", "media group",
        "publishing", "news agency", "press agency",
        "tv channel", "television", "radio",
        # Rugs/Carpets (different machinery, not stenter)
        "rug", "rugs", "carpet backing",
    ]
    
    # Association/Fair domains to block
    DOMAIN_BLOCKLIST = [
        # Certification databases (NOT company sites)
        "global-trace-base.org",
        "oeko-tex.com",
        "services.oeko-tex.com",
        "gots.org",
        "bettercotton.org",
        "wrap.org",
        # Trade associations
        "abit.org.br",
        "texbrasil.com.br",
        "febratex.com.br",
        "itmf.org",
        # Social media
        "instagram.com",
        "facebook.com",
        "linkedin.com",
        "youtube.com",
        "twitter.com",
        # B2B marketplaces
        "indiamart.com",
        "alibaba.com",
        "made-in-china.com",
        "tradekey.com",
        "globalsources.com",
        # Business directories
        "wikipedia.org",
        "emis.com",
        "dnb.com",
        "kompass.com",
        "europages.com",
        "zoominfo.com",
    ]
    
    # Generic terms that shouldn't be company names alone
    GENERIC_TERMS = [
        "textile", "dyeing", "finishing", "knitting", "weaving",
        "fabric", "garment", "apparel", "clothing", "yarn",
        "cotton", "polyester", "denim", "jersey"
    ]
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize cleaner with optional config
        
        Args:
            config: Optional dict with 'noise_keywords' and 'blocked_domains'
        """
        if config:
            self.NOISE_KEYWORDS.extend(config.get('noise_keywords', []))
            self.DOMAIN_BLOCKLIST.extend(config.get('blocked_domains', []))
    
    def is_noise(self, company_name: str) -> bool:
        """
        Detect non-company entries
        
        Examples:
        - "Istanbul event" -> True
        - "Textile" -> True (generic term)
        - "ABC Textiles Ltd" -> False
        - "Pakistan Textile" -> True (country + generic)
        
        Args:
            company_name: Company name to check
            
        Returns:
            True if noise, False if valid company
        """
        if not company_name or len(company_name.strip()) < 3:
            return True
        
        name_lower = company_name.lower().strip()
        words = name_lower.split()
        
        # Single generic word
        if len(words) == 1 and name_lower in self.GENERIC_TERMS:
            logger.debug(f"Noise: Single generic term '{company_name}'")
            return True
        
        # Two words: Country + Generic (e.g., "Pakistan Textile")
        if len(words) == 2:
            if words[1] in self.GENERIC_TERMS or words[0] in self.GENERIC_TERMS:
                logger.debug(f"Noise: Country+Generic pattern '{company_name}'")
                return True
        
        # Check for noise keywords
        noise_patterns = [
            r"\b(event|summit|conference|review|news|expo|fair|exhibition)\b",
            r"^(the |a )?(textile|dyeing|finishing|machine)$",
            r"\d{4}\s*(event|summit|conference)",  # "2024 event"
        ]
        
        for pattern in noise_patterns:
            if re.search(pattern, name_lower):
                logger.debug(f"Noise: Pattern match '{pattern}' in '{company_name}'")
                return True
        
        # Very short names are suspicious
        if len(name_lower) < 5 and not re.search(r'\w+\s+(ltd|inc|llc|gmbh|sa|srl)', name_lower):
            logger.debug(f"Noise: Too short without suffix '{company_name}'")
            return True
        
        return False
    
    def is_non_customer(self, company_name: str, context: str = "") -> bool:
        """
        GPT Audit: Check if entity is NOT a stenter customer.
        
        Filters out:
        - Labels/Packaging companies
        - Plastic/Fiber producers
        - Software companies
        - Machinery suppliers
        - Organizations
        
        Args:
            company_name: Company name
            context: Additional context (description, source)
            
        Returns:
            True if NOT a customer, False if potentially valid customer
        """
        text = f"{company_name} {context}".lower()
        
        for indicator in self.NON_CUSTOMER_INDICATORS:
            if indicator in text:
                # Exception: "garment" is OK if combined with dyeing/finishing
                if indicator == "garment" and any(x in text for x in ["dyeing", "finishing", "boyama", "terbiye", "tinturaria"]):
                    continue
                # V10.5: "institute" is OK if followed by "of technology" (e.g., IIT)
                if indicator == "institute" and "technology" in text:
                    continue
                # V10.5: "chamber" alone should not filter "reaction chamber" etc.
                if indicator == "chamber" and "reaction" in text:
                    continue
                logger.debug(f"Non-customer indicator '{indicator}' found in: {company_name}")
                return True
        
        return False
    
    def validate_domain(self, domain: str) -> bool:
        """
        Check if domain is a valid company website
        
        Returns False for:
        - Association/fair sites
        - Social media
        - Generic portals
        - B2B marketplaces
        
        Args:
            domain: Domain to validate
            
        Returns:
            True if valid company domain, False otherwise
        """
        if not domain:
            return False
        
        # Convert to string and handle non-string types
        if not isinstance(domain, str):
            domain = str(domain)
        
        domain_lower = domain.lower().strip()
        
        # Remove protocol and path
        domain_lower = re.sub(r'^https?://', '', domain_lower)
        domain_lower = domain_lower.split('/')[0]
        
        # Check blocklist
        for blocked in self.DOMAIN_BLOCKLIST:
            if blocked in domain_lower:
                logger.debug(f"Blocked domain: {domain} (matches {blocked})")
                return False
        
        return True
    
    def clean_dataset(self, leads: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """
        Apply all cleaning rules to dataset
        
        Args:
            leads: List of lead dicts with 'company' or 'company_name', 'website', etc.
            
        Returns:
            Tuple of (cleaned_leads, rejected_leads)
        """
        cleaned = []
        rejected = []
        
        for lead in leads:
            # Handle both 'company' and 'company_name' fields
            company_name = lead.get('company_name') or lead.get('company', '')
            if isinstance(company_name, str):
                company_name = company_name.strip()
            else:
                company_name = str(company_name).strip() if company_name else ''
            
            # Filter noise
            if self.is_noise(company_name):
                rejected.append({
                    **lead,
                    'rejection_reason': 'noise_company_name'
                })
                continue
            
            # Clear invalid websites but keep the lead
            website = lead.get('website', '')
            
            # Handle NaN/None values
            if not website or (isinstance(website, float) and str(website) == 'nan'):
                website = None
            elif isinstance(website, str):
                website = website.strip()
            else:
                website = str(website).strip()
            
            if website and not self.validate_domain(website):
                logger.info(f"Clearing invalid domain for {company_name}: {website}")
                lead['website'] = None
                lead['needs_discovery'] = True
                lead['invalid_domain_cleared'] = website
            
            cleaned.append(lead)
        
        logger.info(f"Cleaned {len(leads)} leads: {len(cleaned)} kept, {len(rejected)} rejected")
        
        return cleaned, rejected
    
    def clean_phone(self, phone: str) -> Optional[str]:
        """
        Normalize phone numbers
        
        Args:
            phone: Raw phone string
            
        Returns:
            Normalized phone or None
        """
        if not phone:
            return None
        
        # Remove common separators
        cleaned = re.sub(r'[\s\-\(\)\.]', '', phone)
        
        # Must have at least 7 digits
        if len(re.findall(r'\d', cleaned)) < 7:
            return None
        
        return cleaned
    
    def clean_email(self, email: str) -> Optional[str]:
        """
        Validate email format
        
        Args:
            email: Email string
            
        Returns:
            Cleaned email or None
        """
        if not email:
            return None
        
        email = email.lower().strip()
        
        # Basic email pattern
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        if re.match(pattern, email):
            return email
        
        return None
    
    def get_stats(self, original_count: int, cleaned: List[Dict], rejected: List[Dict]) -> Dict:
        """
        Generate cleaning statistics
        
        Returns:
            Dict with noise rate, invalid domains, etc.
        """
        return {
            'original_count': original_count,
            'cleaned_count': len(cleaned),
            'rejected_count': len(rejected),
            'noise_rate': round(len(rejected) / original_count * 100, 2) if original_count > 0 else 0,
            'domains_cleared': sum(1 for lead in cleaned if lead.get('invalid_domain_cleared')),
            'needs_discovery': sum(1 for lead in cleaned if lead.get('needs_discovery'))
        }
