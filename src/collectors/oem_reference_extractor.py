#!/usr/bin/env python3
"""
OEM Reference Extractor - Extract REAL customer names from OEM news/reference pages
Implements skill: oem-reference-extract
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class CustomerMention:
    """A verified customer mention from OEM source."""
    company: str
    country: str
    oem_brand: str
    equipment_type: str
    evidence_url: str
    evidence_snippet: str
    confidence: str  # high, medium, low


class OEMReferenceExtractor:
    """
    Precision extraction of customer names from OEM manufacturer websites.
    
    Targets: Brückner, Monforts, Krantz, Artos, Santex
    """
    
    # OEM brand names (these are NOT customers)
    OEM_BRANDS = {
        'brückner', 'bruckner', 'brueckner', 'brückner textile',
        'monforts', 'a. monforts', 'monforts textilmaschinen',
        'krantz', 'artos', 'santex', 'santex rimar',
        'babcock', 'strahm', 'goller'
    }
    
    # Equipment keywords that indicate real customer context
    EQUIPMENT_KEYWORDS = {
        'stenter': ['stenter', 'spannrahmen', 'ramöz', 'ram'],
        'montex': ['montex', 'monfortex'],
        'power_frame': ['power-frame', 'power frame', 'supra'],
        'finishing_line': ['finishing line', 'finishing range', 'terbiye hattı'],
        'heat_setting': ['heat setting', 'heat-setting', 'thermofixierung', 'termofiksaj'],
        'coating': ['coating line', 'coating machine', 'kaplama'],
        'drying': ['dryer', 'drying range', 'kurutma']
    }
    
    # Country patterns for extraction
    COUNTRY_PATTERNS = [
        (r'\bin\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b', 1),  # "in Brazil"
        (r'\bfrom\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b', 1),  # "from Turkey"  
        (r'\blocated\s+in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b', 1),
        (r',\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*[,.]', 1),  # ", Brazil,"
    ]
    
    # Known countries for validation
    KNOWN_COUNTRIES = {
        'brazil', 'argentina', 'mexico', 'peru', 'colombia', 'chile', 'ecuador',
        'egypt', 'morocco', 'tunisia', 'algeria', 'libya',
        'turkey', 'türkiye', 'pakistan', 'india', 'bangladesh', 'sri lanka',
        'germany', 'italy', 'spain', 'portugal', 'france', 'uk', 'united kingdom',
        'usa', 'united states', 'china', 'vietnam', 'indonesia', 'thailand'
    }
    
    # Patterns for customer extraction
    CUSTOMER_PATTERNS = [
        # "X in Country has installed/ordered/commissioned"
        r'([A-Z][A-Za-z0-9\s&\.\-]+(?:Ltd|GmbH|SA|Inc|Group|SpA|Ltda|SRL|AS|KG)?)\s+(?:in|from)\s+[A-Z][a-z]+\s+has\s+(?:installed|ordered|commissioned|received)',
        
        # "delivered to X in Country"
        r'delivered\s+to\s+(?:the\s+)?([A-Z][A-Za-z0-9\s&\.\-]+(?:Ltd|GmbH|SA|Inc|Group|SpA|Ltda|SRL|AS|KG)?)\s+(?:in|from)',
        
        # "installed at X's facility/plant"
        r'installed\s+at\s+([A-Z][A-Za-z0-9\s&\.\-]+(?:Ltd|GmbH|SA|Inc|Group|SpA|Ltda|SRL|AS|KG)?)(?:\'s)?\s+(?:facility|plant|factory|mill)',
        
        # "X has commissioned/invested"
        r'([A-Z][A-Za-z0-9\s&\.\-]+(?:Ltd|GmbH|SA|Inc|Group|SpA|Ltda|SRL|AS|KG)?)\s+has\s+(?:commissioned|invested|installed|ordered)',
        
        # "Customer: X" or "Project: X"
        r'(?:Customer|Project|Reference|Client):\s*([A-Z][A-Za-z0-9\s&\.\-]+)',
        
        # Quote attribution: says X, CEO of Company
        r'says\s+[A-Z][a-z]+\s+[A-Z][a-z]+,\s+[A-Z][A-Za-z]+\s+(?:at|of)\s+([A-Z][A-Za-z0-9\s&\.\-]+)',
    ]
    
    def __init__(self, http_client=None):
        self.http_client = http_client
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; LeadIntel/1.0; Research)'
        })
    
    def extract_from_url(self, url: str, oem_brand: str) -> List[CustomerMention]:
        """
        Extract customer mentions from a single URL.
        
        Args:
            url: OEM reference/news page URL
            oem_brand: The OEM brand (bruckner, monforts, etc.)
            
        Returns:
            List of verified CustomerMention objects
        """
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            html = response.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return []
        
        return self.extract_from_html(html, url, oem_brand)
    
    def extract_from_html(self, html: str, source_url: str, oem_brand: str) -> List[CustomerMention]:
        """
        Extract customer mentions from HTML content.
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script, style, nav, footer
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            tag.decompose()
        
        text = soup.get_text(separator=' ', strip=True)
        
        # Find all potential customer mentions
        mentions = []
        
        for pattern in self.CUSTOMER_PATTERNS:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                company_name = match.group(1).strip()
                
                # Get surrounding context (±150 chars)
                start = max(0, match.start() - 150)
                end = min(len(text), match.end() + 150)
                context = text[start:end]
                
                # Validate and clean
                cleaned = self._validate_company_name(company_name)
                if not cleaned:
                    continue
                
                # Extract country from context
                country = self._extract_country(context)
                
                # Detect equipment type
                equipment = self._detect_equipment(context)
                
                # Determine confidence
                confidence = self._calculate_confidence(cleaned, country, equipment, context)
                
                mention = CustomerMention(
                    company=cleaned,
                    country=country,
                    oem_brand=oem_brand,
                    equipment_type=equipment,
                    evidence_url=source_url,
                    evidence_snippet=context[:300],
                    confidence=confidence
                )
                mentions.append(mention)
        
        # Deduplicate by company name
        seen = set()
        unique = []
        for m in mentions:
            key = m.company.lower()
            if key not in seen:
                seen.add(key)
                unique.append(m)
        
        return unique
    
    def _validate_company_name(self, name: str) -> Optional[str]:
        """
        Validate and clean a potential company name.
        
        Returns:
            Cleaned company name, or None if invalid
        """
        # Basic cleaning
        name = re.sub(r'\s+', ' ', name).strip()
        name = re.sub(r'^(the|a|an)\s+', '', name, flags=re.IGNORECASE)
        
        # Too short or too long
        if len(name) < 3 or len(name) > 80:
            return None
        
        # Word count check (1-8 words)
        words = name.split()
        if len(words) < 1 or len(words) > 8:
            return None
        
        # Is it an OEM brand? (not a customer)
        if name.lower() in self.OEM_BRANDS:
            return None
        
        # Is it a generic term?
        generic = {'manufacturer', 'textile', 'finishing', 'dyeing', 'machine', 
                   'equipment', 'process', 'technology', 'industry', 'production'}
        if len(words) == 1 and words[0].lower() in generic:
            return None
        
        # Contains OEM brand at start (likely "Brückner stenter" not a company)
        for oem in self.OEM_BRANDS:
            if name.lower().startswith(oem):
                return None
        
        # Looks like a sentence?
        if any(w in name.lower() for w in ['has', 'have', 'will', 'are', 'is', 'the']):
            if len(words) > 3:
                return None
        
        return name
    
    def _extract_country(self, context: str) -> str:
        """Extract country name from context."""
        for pattern, group in self.COUNTRY_PATTERNS:
            match = re.search(pattern, context)
            if match:
                country = match.group(group)
                if country.lower() in self.KNOWN_COUNTRIES:
                    return country.title()
        
        # Direct country mention
        context_lower = context.lower()
        for country in self.KNOWN_COUNTRIES:
            if country in context_lower:
                return country.title()
        
        return 'Unknown'
    
    def _detect_equipment(self, context: str) -> str:
        """Detect equipment type from context."""
        context_lower = context.lower()
        
        for eq_type, keywords in self.EQUIPMENT_KEYWORDS.items():
            for kw in keywords:
                if kw in context_lower:
                    return eq_type
        
        return 'unknown'
    
    def _calculate_confidence(self, company: str, country: str, 
                              equipment: str, context: str) -> str:
        """Calculate confidence level for the mention."""
        score = 0
        
        # Has company suffix
        if re.search(r'\b(Ltd|GmbH|SA|Inc|Group|SpA|Ltda|SRL|AS|KG)\b', company, re.IGNORECASE):
            score += 2
        
        # Country identified
        if country != 'Unknown':
            score += 2
        
        # Equipment type identified
        if equipment != 'unknown':
            score += 1
        
        # Strong action words in context
        strong_words = ['installed', 'commissioned', 'delivered', 'ordered', 'invested']
        if any(w in context.lower() for w in strong_words):
            score += 1
        
        # Multi-word company name
        if len(company.split()) >= 2:
            score += 1
        
        if score >= 5:
            return 'high'
        elif score >= 3:
            return 'medium'
        else:
            return 'low'
    
    def to_leads(self, mentions: List[CustomerMention]) -> List[Dict]:
        """Convert CustomerMention objects to lead dictionaries."""
        leads = []
        for m in mentions:
            leads.append({
                'company': m.company,
                'country': m.country,
                'source_type': 'oem_customer',
                'source_url': m.evidence_url,
                'oem_reference': m.oem_brand,
                'equipment_type': m.equipment_type,
                'context': m.evidence_snippet,
                'confidence': m.confidence,
                'brand_mentioned': m.oem_brand.title()
            })
        return leads


# Test/demo
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    extractor = OEMReferenceExtractor()
    
    # Test with sample text
    sample = """
    TEXCOM of Argentina has recently installed a new BRÜCKNER POWER-FRAME stenter 
    at their facility in Buenos Aires. The company, known for high-quality textile 
    finishing, has been using Brückner equipment for over a decade.
    
    Meanwhile, GRUPO MALWEE in Brazil has commissioned their third Brückner stenter 
    line, demonstrating continued trust in German engineering.
    
    In Turkey, Altun Tekstil A.Ş. has ordered a complete Monforts Montex finishing 
    range for their Bursa plant.
    """
    
    # Simulate extraction
    mentions = extractor.extract_from_html(
        f"<html><body>{sample}</body></html>",
        "https://example.com/news",
        "brückner"
    )
    
    print(f"Found {len(mentions)} customer mentions:")
    for m in mentions:
        print(f"  ✓ {m.company} ({m.country}) - {m.equipment_type} [{m.confidence}]")
