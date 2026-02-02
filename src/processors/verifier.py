"""
GPT Fix #4: Phone/Email Verifier Module

Validates contact information for sales readiness:
- Email domain matches website domain
- Phone country code matches company country
- Marks confidence levels (high, medium, low)
"""

import re
from urllib.parse import urlparse

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Country phone codes
COUNTRY_PHONE_CODES = {
    "brazil": ["+55", "55"],
    "argentina": ["+54", "54"],
    "colombia": ["+57", "57"],
    "ecuador": ["+593", "593"],
    "peru": ["+51", "51"],
    "chile": ["+56", "56"],
    "mexico": ["+52", "52"],
    "turkey": ["+90", "90"],
    "egypt": ["+20", "20"],
    "morocco": ["+212", "212"],
    "tunisia": ["+216", "216"],
    "india": ["+91", "91"],
    "bangladesh": ["+880", "880"],
    "pakistan": ["+92", "92"],
    "vietnam": ["+84", "84"],
    "indonesia": ["+62", "62"],
    "germany": ["+49", "49"],
    "italy": ["+39", "39"],
    "spain": ["+34", "34"],
    "portugal": ["+351", "351"],
    "france": ["+33", "33"],
    "usa": ["+1", "1"],
    "united states": ["+1", "1"],
    "china": ["+86", "86"],
}

# Common TLDs by country
COUNTRY_TLDS = {
    "brazil": [".br", ".com.br"],
    "argentina": [".ar", ".com.ar"],
    "colombia": [".co", ".com.co"],
    "ecuador": [".ec", ".com.ec"],
    "peru": [".pe", ".com.pe"],
    "chile": [".cl", ".com.cl"],
    "mexico": [".mx", ".com.mx"],
    "turkey": [".tr", ".com.tr"],
    "egypt": [".eg", ".com.eg"],
    "morocco": [".ma", ".com.ma"],
    "germany": [".de", ".com.de"],
}


class ContactVerifier:
    """Verifies and scores contact information confidence."""

    def __init__(self, settings=None):
        self.settings = settings or {}

    def verify_lead(self, lead):
        """
        Verify all contacts for a lead and add confidence scores.
        
        Returns modified lead with:
        - email_confidence: 'high', 'medium', 'low'
        - phone_confidence: 'high', 'medium', 'low'
        - verified_emails: list of high-confidence emails
        - verified_phones: list of high-confidence phones
        - verification_notes: list of issues found
        """
        notes = []
        
        website = lead.get("website") or ""
        website_domain = self._extract_domain(website)
            # Safe string conversion for country
        country_raw = lead.get("country")
        if isinstance(country_raw, float): # Handle NaN
             country = ""
        else:
             country = str(country_raw or "").lower().strip()
        
        # Verify emails
        emails = self._parse_list(lead.get("emails", []))
        verified_emails = []
        email_confidence = "low"
        
        for email in emails:
            conf, note = self._verify_email(email, website_domain, country)
            if conf == "high":
                verified_emails.append(email)
                email_confidence = "high"
            elif conf == "medium" and email_confidence != "high":
                email_confidence = "medium"
            if note:
                notes.append(note)
        
        # Verify phones
        phones = self._parse_list(lead.get("phones", []))
        verified_phones = []
        phone_confidence = "low"
        
        for phone in phones:
            conf, note = self._verify_phone(phone, country)
            if conf == "high":
                verified_phones.append(phone)
                phone_confidence = "high"
            elif conf == "medium" and phone_confidence != "high":
                phone_confidence = "medium"
            if note:
                notes.append(note)
        
        # Update lead
        lead["email_confidence"] = email_confidence
        lead["phone_confidence"] = phone_confidence
        lead["verified_emails"] = verified_emails
        lead["verified_phones"] = verified_phones
        lead["verification_notes"] = notes
        
        # Overall contact confidence
        if email_confidence == "high" or phone_confidence == "high":
            lead["contact_confidence"] = "high"
        elif email_confidence == "medium" or phone_confidence == "medium":
            lead["contact_confidence"] = "medium"
        else:
            lead["contact_confidence"] = "low"
        
        return lead

    def _verify_email(self, email, website_domain, country):
        """
        Verify email against website domain.
        
        Returns: (confidence, note)
        """
        if not email or "@" not in email:
            return "low", None
        
        email_domain = email.split("@")[-1].lower().strip()
        
        # Check if email domain matches website domain
        if website_domain:
            # Normalize domains for comparison
            email_base = self._get_base_domain(email_domain)
            website_base = self._get_base_domain(website_domain)
            
            if email_base == website_base:
                return "high", None
            
            # Check if subdomain relationship
            if email_domain.endswith(f".{website_base}") or website_domain.endswith(f".{email_base}"):
                return "high", None
            
            # Domain mismatch
            return "low", f"Email domain {email_domain} doesn't match website {website_domain}"
        
        # No website to compare, check if corporate domain
        from src.processors.enricher import FREE_EMAIL_DOMAINS
        if email_domain in FREE_EMAIL_DOMAINS:
            return "low", f"Free email provider: {email_domain}"
        
        # Corporate email but no website to verify
        return "medium", f"Corporate email {email_domain} but no website to verify"

    def _verify_phone(self, phone, country):
        """
        Verify phone country code matches company country.
        
        Returns: (confidence, note)
        """
        if not phone:
            return "low", None
        
        # Clean phone number
        phone_clean = re.sub(r'[^\d+]', '', str(phone))
        
        if not country:
            return "medium", "No country to verify phone against"
        
        # Get expected country codes
        expected_codes = COUNTRY_PHONE_CODES.get(country, [])
        if not expected_codes:
            return "medium", f"Unknown country code pattern for {country}"
        
        # Check if phone starts with expected country code
        for code in expected_codes:
            code_clean = code.replace("+", "")
            if phone_clean.startswith(f"+{code_clean}") or phone_clean.startswith(code_clean):
                return "high", None
        
        # Check what country code the phone has
        detected_country = None
        for ctry, codes in COUNTRY_PHONE_CODES.items():
            for code in codes:
                code_clean = code.replace("+", "")
                if phone_clean.startswith(f"+{code_clean}") or phone_clean.startswith(code_clean):
                    detected_country = ctry
                    break
            if detected_country:
                break
        
        if detected_country and detected_country != country:
            return "low", f"Phone code suggests {detected_country}, company is in {country}"
        
        return "medium", f"Could not verify phone {phone} for {country}"

    def _extract_domain(self, url):
        """Extract domain from URL."""
        if not url:
            return ""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if not domain and "/" not in url:
                domain = url.lower()
            return domain.replace("www.", "")
        except:
            return ""

    def _get_base_domain(self, domain):
        """Get base domain without subdomain."""
        if not domain:
            return ""
        parts = domain.split(".")
        if len(parts) >= 2:
            # Handle .com.xx, .co.xx etc
            if parts[-2] in ["com", "co", "org", "net", "edu", "gov"]:
                if len(parts) >= 3:
                    return ".".join(parts[-3:])
            return ".".join(parts[-2:])
        return domain

    def _parse_list(self, value):
        """Parse value that might be string representation of list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            value = value.strip()
            if value.startswith("["):
                try:
                    import ast
                    return ast.literal_eval(value)
                except:
                    pass
            if value and value not in {"nan", "None", "[]"}:
                return [value]
        return []


def verify_leads(leads, settings=None):
    """Convenience function to verify a list of leads."""
    verifier = ContactVerifier(settings)
    return [verifier.verify_lead(lead) for lead in leads]
