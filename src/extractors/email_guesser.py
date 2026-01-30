# =============================================================================
# EMAIL GUESSER - V10 Pattern-Based Email Discovery
# =============================================================================
# Purpose: Generate probable email addresses based on company domain and
#          country-specific email patterns
# 
# Patterns by Region:
# - Latin America: ventas@, comercial@, export@, contacto@
# - Europe: sales@, info@, export@, contact@
# - Middle East/Asia: info@, sales@, export@
# 
# This avoids scraping personal emails and instead targets department emails
# which are more likely to reach decision makers.
# =============================================================================

import re
from dataclasses import dataclass
from typing import List, Optional, Dict, Set
from pathlib import Path
import yaml


@dataclass
class EmailGuess:
    """A guessed email address with confidence"""
    email: str
    pattern_type: str  # 'department', 'role', 'generic'
    confidence: str    # 'high', 'medium', 'low'
    language: str      # 'es', 'pt', 'en', 'de', 'fr', 'tr', 'ar'
    priority: int      # 1-10, higher = try first


class EmailGuesser:
    """
    Pattern-based email address guesser.
    
    Given a company domain and country, generates probable email addresses
    for sales/export departments.
    
    Features:
    - Region-specific patterns (LA, EU, MENA)
    - Language-appropriate prefixes
    - Prioritized output for testing order
    - Domain validation
    """
    
    def __init__(self):
        self._build_patterns()
        
    def _build_patterns(self) -> None:
        """Build email patterns by language/region"""
        
        # === SPANISH (Latin America + Spain) ===
        self.es_patterns = [
            ("ventas", "department", "high", 10),
            ("comercial", "department", "high", 9),
            ("exportacion", "department", "high", 8),
            ("export", "department", "high", 8),
            ("contacto", "generic", "medium", 7),
            ("info", "generic", "medium", 6),
            ("atencioncliente", "department", "medium", 5),
            ("compras", "department", "medium", 5),
            ("gerencia", "role", "low", 3),
        ]
        
        # === PORTUGUESE (Brazil + Portugal) ===
        self.pt_patterns = [
            ("vendas", "department", "high", 10),
            ("comercial", "department", "high", 9),
            ("exportacao", "department", "high", 8),
            ("export", "department", "high", 8),
            ("contato", "generic", "medium", 7),
            ("info", "generic", "medium", 6),
            ("atendimento", "department", "medium", 5),
            ("compras", "department", "medium", 5),
        ]
        
        # === ENGLISH (Global) ===
        self.en_patterns = [
            ("sales", "department", "high", 10),
            ("export", "department", "high", 9),
            ("info", "generic", "high", 8),
            ("contact", "generic", "medium", 7),
            ("enquiry", "generic", "medium", 6),
            ("enquiries", "generic", "medium", 6),
            ("hello", "generic", "medium", 5),
            ("support", "department", "low", 4),
        ]
        
        # === GERMAN (DACH) ===
        self.de_patterns = [
            ("vertrieb", "department", "high", 10),
            ("verkauf", "department", "high", 9),
            ("export", "department", "high", 8),
            ("info", "generic", "high", 8),
            ("kontakt", "generic", "medium", 7),
            ("anfrage", "generic", "medium", 6),
            ("einkauf", "department", "medium", 5),
        ]
        
        # === FRENCH (France, Tunisia, Algeria, Morocco) ===
        self.fr_patterns = [
            ("commercial", "department", "high", 10),
            ("ventes", "department", "high", 9),
            ("export", "department", "high", 8),
            ("info", "generic", "high", 8),
            ("contact", "generic", "medium", 7),
            ("direction", "role", "low", 4),
        ]
        
        # === TURKISH ===
        self.tr_patterns = [
            ("satis", "department", "high", 10),
            ("ihracat", "department", "high", 9),
            ("info", "generic", "high", 8),
            ("iletisim", "generic", "medium", 7),
            ("destek", "department", "medium", 5),
        ]
        
        # === ARABIC REGIONS (transliterated + English) ===
        self.ar_patterns = [
            ("sales", "department", "high", 10),
            ("export", "department", "high", 9),
            ("info", "generic", "high", 8),
            ("contact", "generic", "medium", 7),
        ]
        
        # Country → Language mapping
        self.country_language = {
            # South America
            "argentina": "es",
            "chile": "es",
            "colombia": "es",
            "ecuador": "es",
            "peru": "es",
            "mexico": "es",
            "venezuela": "es",
            "uruguay": "es",
            "paraguay": "es",
            "bolivia": "es",
            
            "brazil": "pt",
            
            # Europe
            "germany": "de",
            "austria": "de",
            "switzerland": "de",  # Also fr, it
            
            "france": "fr",
            
            "spain": "es",
            "portugal": "pt",
            
            "italy": "en",  # Often use English for export
            "netherlands": "en",
            "belgium": "en",
            "uk": "en",
            "ireland": "en",
            
            "turkey": "tr",
            
            # North Africa / Middle East
            "tunisia": "fr",
            "algeria": "fr",
            "morocco": "fr",
            "egypt": "ar",
            
            # Asia
            "india": "en",
            "pakistan": "en",
            "bangladesh": "en",
            "china": "en",
            
            # Default
            "default": "en",
        }
        
        # Pattern sets by language
        self.patterns_by_lang = {
            "es": self.es_patterns,
            "pt": self.pt_patterns,
            "en": self.en_patterns,
            "de": self.de_patterns,
            "fr": self.fr_patterns,
            "tr": self.tr_patterns,
            "ar": self.ar_patterns,
        }
        
        # Free email domains (exclude these)
        self.free_email_domains = {
            "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
            "live.com", "aol.com", "icloud.com", "mail.com",
            "protonmail.com", "zoho.com", "yandex.com",
            # Regional
            "gmx.de", "web.de", "t-online.de",  # Germany
            "orange.fr", "free.fr", "sfr.fr",   # France
            "uol.com.br", "bol.com.br", "terra.com.br",  # Brazil
        }
        
    def guess(self, domain: str, country: str = None,
              include_generic: bool = True,
              max_results: int = 10) -> List[EmailGuess]:
        """
        Generate probable email addresses for a company.
        
        Args:
            domain: Company domain (e.g., "example.com")
            country: Country name for language-specific patterns
            include_generic: Include info@, contact@, etc.
            max_results: Maximum number of guesses to return
            
        Returns:
            List of EmailGuess sorted by priority
        """
        # Clean domain
        domain = self._clean_domain(domain)
        
        if not domain or domain in self.free_email_domains:
            return []
            
        # Determine language
        country_lower = (country or "").lower().strip()
        language = self.country_language.get(country_lower, "en")
        
        # Get patterns for this language
        patterns = self.patterns_by_lang.get(language, self.en_patterns)
        
        # Also include English patterns as fallback
        if language != "en":
            patterns = patterns + self.en_patterns
            
        # Generate emails
        guesses = []
        seen_prefixes = set()
        
        for prefix, pattern_type, confidence, priority in patterns:
            if prefix in seen_prefixes:
                continue
                
            if not include_generic and pattern_type == "generic":
                continue
                
            seen_prefixes.add(prefix)
            
            email = f"{prefix}@{domain}"
            
            guesses.append(EmailGuess(
                email=email,
                pattern_type=pattern_type,
                confidence=confidence,
                language=language,
                priority=priority
            ))
            
        # Sort by priority (highest first)
        guesses.sort(key=lambda x: -x.priority)
        
        return guesses[:max_results]
        
    def _clean_domain(self, domain: str) -> Optional[str]:
        """Extract clean domain from various input formats"""
        if not domain:
            return None
        
        # Handle NaN/float values
        if isinstance(domain, float):
            return None
            
        domain = str(domain).strip().lower()
        
        if domain == 'nan' or domain == 'none' or domain == '':
            return None
        
        # Remove protocol
        domain = re.sub(r'^https?://', '', domain)
        
        # Remove www.
        domain = re.sub(r'^www\.', '', domain)
        
        # Remove path
        domain = domain.split('/')[0]
        
        # Remove port
        domain = domain.split(':')[0]
        
        # Validate domain format
        if not re.match(r'^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z]{2,})+$', domain):
            return None
            
        return domain
        
    def guess_from_website(self, website: str, country: str = None) -> List[EmailGuess]:
        """
        Convenience method: guess emails from website URL.
        
        Args:
            website: Full website URL or domain
            country: Country for language-specific patterns
            
        Returns:
            List of EmailGuess sorted by priority
        """
        domain = self._clean_domain(website)
        if not domain:
            return []
        return self.guess(domain, country)
        
    def guess_for_lead(self, lead: Dict) -> List[EmailGuess]:
        """
        Generate email guesses for a lead dictionary.
        
        Expects lead to have 'website' or 'domain' and optionally 'country'.
        
        Args:
            lead: Lead dictionary
            
        Returns:
            List of EmailGuess sorted by priority
        """
        website = lead.get("website") or lead.get("domain") or lead.get("url")
        country = lead.get("country", "")
        
        if not website:
            return []
            
        return self.guess_from_website(website, country)
        
    def prioritize_by_role(self, guesses: List[EmailGuess],
                           target_roles: List[str] = None) -> List[EmailGuess]:
        """
        Re-prioritize guesses based on target roles.
        
        Args:
            guesses: List of email guesses
            target_roles: Preferred roles like ['sales', 'export', 'commercial']
            
        Returns:
            Re-sorted list with target roles first
        """
        if not target_roles:
            target_roles = ["sales", "export", "ventas", "vendas", "commercial"]
            
        def role_priority(guess: EmailGuess) -> int:
            prefix = guess.email.split("@")[0]
            for i, role in enumerate(target_roles):
                if role in prefix:
                    return i
            return len(target_roles) + guess.priority
            
        return sorted(guesses, key=role_priority)
        
    def format_for_outreach(self, guesses: List[EmailGuess],
                            include_confidence: bool = False) -> List[str]:
        """
        Format guesses as simple email strings for outreach.
        
        Args:
            guesses: List of email guesses
            include_confidence: Add confidence marker to output
            
        Returns:
            List of email strings
        """
        if include_confidence:
            return [
                f"{g.email} ({g.confidence})"
                for g in guesses
            ]
        return [g.email for g in guesses]


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

# Global guesser instance
_guesser: Optional[EmailGuesser] = None


def get_guesser() -> EmailGuesser:
    """Get or create global guesser instance"""
    global _guesser
    if _guesser is None:
        _guesser = EmailGuesser()
    return _guesser


def guess_emails(domain: str, country: str = None,
                 max_results: int = 5) -> List[str]:
    """
    Quick email guessing function.
    
    Usage:
        emails = guess_emails("example.com", "Brazil")
        # Returns: ['vendas@example.com', 'comercial@example.com', ...]
    """
    guesser = get_guesser()
    guesses = guesser.guess(domain, country, max_results=max_results)
    return [g.email for g in guesses]


def guess_emails_for_leads(leads: List[Dict],
                           top_n: int = 3) -> List[Dict]:
    """
    Add email guesses to a list of leads.
    
    Args:
        leads: List of lead dictionaries with 'website' and 'country'
        top_n: Number of email guesses per lead
        
    Returns:
        Leads with 'guessed_emails' field added
    """
    guesser = get_guesser()
    
    for lead in leads:
        guesses = guesser.guess_for_lead(lead)
        lead["guessed_emails"] = [g.email for g in guesses[:top_n]]
        
    return leads


# =============================================================================
# CLI INTERFACE
# =============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) >= 2:
        domain = sys.argv[1]
        country = sys.argv[2] if len(sys.argv) >= 3 else None
        
        guesser = EmailGuesser()
        guesses = guesser.guess(domain, country)
        
        print(f"\nEmail guesses for {domain}" + (f" ({country})" if country else ""))
        print("=" * 50)
        
        for g in guesses:
            conf_marker = {"high": "🟢", "medium": "🟡", "low": "🟠"}[g.confidence]
            print(f"{conf_marker} {g.email}")
            print(f"   Type: {g.pattern_type} | Lang: {g.language} | Priority: {g.priority}")
            
    else:
        # Demo mode
        print("\n" + "=" * 60)
        print("EMAIL GUESSER DEMO")
        print("=" * 60)
        
        test_cases = [
            ("textilbrasil.com.br", "Brazil"),
            ("textilesargentina.com.ar", "Argentina"),
            ("finishing-tunisia.tn", "Tunisia"),
            ("textilmaschinen.de", "Germany"),
            ("mills-egypt.com", "Egypt"),
            ("fabricas-mexico.com.mx", "Mexico"),
        ]
        
        guesser = EmailGuesser()
        
        for domain, country in test_cases:
            guesses = guesser.guess(domain, country, max_results=5)
            
            print(f"\n📧 {domain} ({country})")
            for g in guesses:
                print(f"   → {g.email} [{g.confidence}]")
                
        print("\n" + "=" * 60)
        print("Usage: python email_guesser.py <domain> [country]")
        print("Example: python email_guesser.py example.com.br Brazil")
