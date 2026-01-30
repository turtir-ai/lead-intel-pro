#!/usr/bin/env python3
"""
Entity Quality Gate v2 - Advanced filter for non-company entities
Based on project_v4.md requirements

Improvements over v1:
- Title/headline detection (colon patterns, "Types", "Machine:")
- Sentence fragment detection (We are, fully supported, etc.)
- Person/role detection (Technologist, Manager, Director)
- Machine name detection (Brückner Stenter, Monforts Stenter)
- Evidence requirement scoring
"""

import re
import logging
from typing import Dict, List, Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class EntityQualityGateV2:
    """
    Advanced entity filtering with v4 requirements.
    
    Quality Grades:
    - A: High confidence (company suffix + website + evidence)
    - B: Medium confidence (2+ words + domain OR directory source)
    - C: Low confidence (needs manual review)
    - REJECT: Not a company (article, machine, person, sentence)
    """
    
    # Company suffixes indicating real entity - ONLY LEGAL FORM SUFFIXES
    # GPT Audit Fix: Removed generic industry nouns (textile, fabric, mill, finishing)
    # These are NOT company suffixes - they are industry descriptors
    COMPANY_SUFFIXES = re.compile(
        r'\b(gmbh|ltd|llc|inc|corp|sa\b|s\.a\.|ag\b|a\.g\.|kg\b|k\.g\.|'
        r'co\.|company|limited|plc|pty|bv\b|b\.v\.|nv\b|n\.v\.|'
        r'srl|s\.r\.l\.|spa|s\.p\.a\.|as\b|a\.s\.|oy|ab\b|'
        r'anonim|a\.ş\.|ltd\.şti\.|ltda|cia|'
        r'industries|group|holdings|bros|brothers|sons|e\.?\s*cia)\b',
        re.IGNORECASE
    )
    
    # OEM garbage entity patterns - article titles, news headlines extracted as "companies"
    # GPT Audit Fix: These come from brueckner_monforts.py regex being too permissive
    OEM_GARBAGE_PATTERNS = re.compile(
        r'^(Advanced|Premium|Value|Booming|Fascinating|Global|Technical|Modern|New|Latest|'
        r'Industrial|Efficient|Sustainable|Innovative|Leading|Major)\s+'
        r'(Textile|Textiles|Finishing|Dyeing|Fabric|Fabrics|Processing|Manufacturing)s?\b|'
        r'\bTextile\s+(Manufacturers?|Industry|Sector|Production|World|Technology)\b|'
        r'\b(CKNER|NFORTS|ANTEX)\s+|'  # Truncated OEM names
        r'^(For|The|In|On|At|To|Of|With)\s+\w+\s+(Textile|Finishing)|'
        r'\bEnergy\s+Efficiencies?\s+For\b|'
        r'\bMachine\s+Orders?\s+(From|To)\b|'
        r'\bPuts?\s+A\s+Premium\b|'
        r'\bThermatron\s+The\b',
        re.IGNORECASE
    )
    
    # Domains to reject
    REJECT_DOMAINS = {
        'alibaba.com', 'aliexpress.com', 'indiamart.com', 'made-in-china.com',
        'globalsources.com', 'ec21.com', 'tradekey.com', 'dhgate.com',
        'wikipedia.org', 'youtube.com', 'facebook.com', 'linkedin.com',
        'twitter.com', 'instagram.com', 'pinterest.com', 'reddit.com',
        'sciencedirect.com', 'researchgate.net', 'academia.edu',
        'textileworld.com', 'fibre2fashion.com', 'apparelresources.com'  # News sites
    }
    
    # Generic terms that alone are NOT company names
    GENERIC_TERMS = {
        'manufacturer', 'manufacturing', 'textile', 'textiles', 'fabric', 'fabrics',
        'finishing', 'dyeing', 'bleaching', 'printing', 'processing',
        'machine', 'machinery', 'equipment', 'technology', 'process',
        'industry', 'industrial', 'production', 'products', 'product',
        'stenter', 'tenter', 'ram', 'stenters', 'tenters', 'frame', 'frames',
        'unknown', 'other', 'various', 'multiple', 'general', 'n/a', 'na',
        'international', 'traditional', 'better', 'deciphering', 'science', 'digital',
        'sons', 'corporation', 'company', 'enterprise', 'enterprises', 'data',
    }
    
    # Website navigation / menu items (definitely NOT companies)
    WEBSITE_MENU_ITEMS = {
        'about us', 'about', 'contact', 'contact us', 'home', 'products', 'services',
        'careers', 'news', 'blog', 'faq', 'privacy', 'terms', 'login', 'register',
        'search', 'menu', 'main', 'overview', 'gallery', 'team', 'history',
    }
    
    # Country names that with S.A./Ltd are too generic
    COUNTRY_NAMES = {
        'chile', 'brazil', 'argentina', 'peru', 'colombia', 'ecuador', 'mexico',
        'turkey', 'india', 'pakistan', 'bangladesh', 'egypt', 'morocco', 'tunisia',
        'italy', 'germany', 'spain', 'portugal', 'france', 'china', 'vietnam',
    }
    
    # === NEW v2 PATTERNS ===
    
    # Title/headline patterns (likely article titles, not companies)
    TITLE_PATTERNS = re.compile(
        r'^.{0,50}:\s|'                    # Colon near start (headline style)
        r'\bTypes?,\s|'                     # "Type," or "Types,"
        r'\bMachine:\s|'                    # "Machine: "
        r'\bGuide\s+(to|for)\b|'           # "Guide to/for"
        r'\bHow\s+to\b|'                   # "How to"
        r'\bWhat\s+is\b|'                  # "What is"
        r'\bIntroduction\s+to\b|'          # "Introduction to"
        r'\b(Top|Best|Latest)\s+\d+\b|'    # "Top 10", "Best 5"
        r'^The\s+(New|Latest|Best)\b|'     # "The New..."
        r':$|'                              # Trailing colon (truncated title)
        r'^Textile\s+Stenter\b|'           # "Textile Stenter..." article title
        r'\bMachine$|'                      # Ends with just "Machine"
        r',\s*(What|How|Why|Who|When|Where)\b|'  # Comma + question word (partial title)
        r'^\w+,\s+(What|How|Why)\b|'       # "Word, What..." pattern
        r'\bFrame$',                        # Ends with "Frame" (machine part)
        re.IGNORECASE
    )
    
    # Sentence fragment patterns (partial sentences, not names)
    SENTENCE_PATTERNS = re.compile(
        r'^(We|They|Our|It|This|That)\s+(are|is|was|were|have|has|will)\b|'  # Subject + verb
        r'\b(fully|now|recently|currently)\s+(supported|available|launched)\b|'  # Adverb phrases
        r'^(Here|There|Now|Also)\s+|'      # Sentence starters
        r'\band\s+more\b|'                 # "and more"
        r'\betc\.?\b|'                     # "etc"
        r'^\d+\s+years?\b|'                # "5 years..."
        r'\bsince\s+\d{4}\b|'              # "since 2020"
        r'\s+at\s+the\s+|'                 # "at the" (location phrase)
        r'\s+in\s+the\s+|'                 # "in the" (location phrase)
        r'\s+plant\s+o[fn]?\b|'            # "plant of" truncated
        r'\s+factory\s+o[fn]?\b|'          # "factory of" truncated
        r'\s+facility\s+o[fn]?\b',          # "facility of" truncated
        re.IGNORECASE
    )
    
    # Person/role patterns (job titles, not companies)
    PERSON_PATTERNS = re.compile(
        r'\b(Technologist|Manager|Director|Engineer|Specialist|Consultant|'
        r'Expert|Officer|Head|Chief|Supervisor|Coordinator|Analyst|'
        r'CEO|CFO|CTO|COO|Owner|Founder|President|VP|Vice)\b|'
        r'\bfor\s+(Dyeing|Finishing|Production|Quality|Sales|Marketing)\b',
        re.IGNORECASE
    )
    
    # Machine/product name patterns (OEM + machine type, not customer)
    MACHINE_PATTERNS = re.compile(
        r'^(Brückner|Monforts|Krantz|Artos|Santex|Babcock|Goller)\s+'
        r'(Stenter|Tenter|Machine|Ram|Range|Line|System|Equipment)\b|'
        r'^Stenter\s+(Machine|Frame|Range|Line)|'
        r'^(Horizontal|Vertical)\s+(Chain|Kette)|'
        r'\bGleitstein\b|\bKluppen\b|\bBuchse\b',  # Product names
        re.IGNORECASE
    )
    
    # Stenter machine manufacturers - REJECT these as they are NOT our customers
    # They are either competitors (other stenter OEMs) or our own company (Brückner)
    STENTER_OEM_MANUFACTURERS = {
        'brückner', 'bruckner', 'monforts', 'krantz', 'artos', 'santex', 
        'babcock', 'goller', 'dilmenler', 'benninger', 'ruckh', 'santex rimar',
        'monforts textilmaschinen', 'brückner textile', 'brückner textil'
    }
    
    # News/media sites - NOT companies
    NEWS_MEDIA_SITES = {
        'texdata', 'texdata international', 'textileworld', 'textile world',
        'fibre2fashion', 'apparel resources', 'just-style', 'fashionunited',
        'textile magazine', 'textile today', 'textile focus', 'textile excellence'
    }
    
    # Parts supplier/trading company keywords - NOT ideal customers
    # These are intermediaries, not end-users with finishing equipment
    PARTS_SUPPLIER_PATTERNS = re.compile(
        r'\b(spare\s*parts?|parts?\s*supplier|parts?\s*trading|'
        r'machinery\s*parts?|machine\s*parts?|spares?\s*stock|'
        r'trading\s*company|trading\s*co|import\s*export|'
        r'consultancy|consulting|solutions?\s*provider|'
        r'machinery\s*dealer|equipment\s*dealer|reseller|distributor)\b',
        re.IGNORECASE
    )
    
    # Country normalization map
    COUNTRY_NORMALIZE = {
        'türkiye': 'Turkey',
        'turkiye': 'Turkey',
        'turkey': 'Turkey',
        'brasil': 'Brazil',
        'brazil': 'Brazil',
        'brezilya': 'Brazil',
        'mısır': 'Egypt',
        'misir': 'Egypt',
        'egypt': 'Egypt',
        'fas': 'Morocco',
        'morocco': 'Morocco',
        'tunus': 'Tunisia',
        'tunisia': 'Tunisia',
        'hindistan': 'India',
        'india': 'India',
        'çin': 'China',
        'china': 'China',
        'pakistan': 'Pakistan',
        'bangladeş': 'Bangladesh',
        'bangladesh': 'Bangladesh',
        'arjantin': 'Argentina',
        'argentina': 'Argentina',
        'peru': 'Peru',
        'kolombiya': 'Colombia',
        'colombia': 'Colombia',
        'şili': 'Chile',
        'chile': 'Chile',
        'ekvador': 'Ecuador',
        'ecuador': 'Ecuador',
        'meksika': 'Mexico',
        'mexico': 'Mexico',
        'almanya': 'Germany',
        'germany': 'Germany',
        'italya': 'Italy',
        'italy': 'Italy',
        'portekiz': 'Portugal',
        'portugal': 'Portugal',
        'ispanya': 'Spain',
        'spain': 'Spain',
    }
    
    # High confidence source types
    # GPT Audit Fix: REMOVED 'oem_customer' - it produces too much garbage
    # oem_customer should NOT get automatic trust; needs website/evidence validation
    HIGH_CONFIDENCE_SOURCES = {
        'known_manufacturer', 'association_member',
        'gots', 'oekotex', 'fair_exhibitor', 'directory',
        'precision_search', 'facility_verified'
    }
    
    # Medium confidence - requires additional validation
    MEDIUM_CONFIDENCE_SOURCES = {
        'oem_customer', 'brave_search', 'fair'
    }
    
    def __init__(self):
        self.reject_count = 0
        self.grade_counts = {'A': 0, 'B': 0, 'C': 0, 'REJECT': 0}
        self.rejection_reasons = {}
    
    def grade_entity(self, lead: Dict) -> Tuple[str, str]:
        """
        Grade an entity's quality.
        
        Returns:
            Tuple of (grade: A/B/C/REJECT, reason: str)
        """
        company = str(lead.get('company', '')).strip()
        source_url = str(lead.get('source_url', lead.get('source', '')))
        source_type = str(lead.get('source_type', '')).lower()
        website = str(lead.get('website', ''))
        evidence_url = str(lead.get('evidence_url', ''))
        
        # Empty company name
        if not company or company.lower() in ('nan', 'none', '', 'null'):
            return 'REJECT', 'Empty company name'
        
        # Check rejection rules first
        reject_reason = self._check_rejection_v2(company, source_url, source_type)
        if reject_reason:
            self._track_rejection(reject_reason)
            return 'REJECT', reject_reason
        
        # Now grade the entity
        grade_score = 0
        reasons = []
        
        # Company suffix check (+2)
        if self.COMPANY_SUFFIXES.search(company):
            grade_score += 2
            reasons.append('Has company suffix')
        
        # Word count check
        word_count = len(company.split())
        if word_count >= 2:
            grade_score += 1
            reasons.append(f'{word_count} words')
        elif word_count == 1 and len(company) < 4:
            grade_score -= 1
            reasons.append('Very short name')
        
        # High confidence source (+2)
        if source_type in self.HIGH_CONFIDENCE_SOURCES:
            grade_score += 2
            reasons.append(f'High confidence: {source_type}')
        
        # GPT Audit Fix: oem_customer only gets bonus if it has website or evidence
        # Otherwise it's just a brave search result with garbage entity extraction
        elif source_type in self.MEDIUM_CONFIDENCE_SOURCES:
            # Only give bonus if there's supporting evidence
            if website and website.lower() not in ('nan', 'none', '', '[]'):
                grade_score += 1
                reasons.append(f'Medium confidence: {source_type} + website')
            elif evidence_url and evidence_url.lower() not in ('nan', 'none', '', '[]'):
                grade_score += 1
                reasons.append(f'Medium confidence: {source_type} + evidence')
            else:
                reasons.append(f'Low trust: {source_type} (no verification)')
        
        # Has website (+1)
        if website and website.lower() not in ('nan', 'none', '', '[]'):
            grade_score += 1
            reasons.append('Has website')
        
        # Has evidence URL (+1)
        if evidence_url and evidence_url.lower() not in ('nan', 'none', '', '[]'):
            grade_score += 1
            reasons.append('Has evidence')
        
        # Determine grade
        if grade_score >= 4:
            grade = 'A'
        elif grade_score >= 2:
            grade = 'B'
        else:
            grade = 'C'
        
        self.grade_counts[grade] += 1
        return grade, '; '.join(reasons) if reasons else 'Default grade'
    
    def _check_rejection_v2(self, company: str, source_url: str, source_type: str) -> Optional[str]:
        """
        Enhanced rejection checks for v2.
        """
        company_lower = company.lower().strip()
        
        # === V5 FIX: Always check for garbage, even from "trusted" sources ===
        # oem_customer and known_manufacturer can still produce garbage entities
        
        # 0. Minimum length check
        if len(company.strip()) < 3:
            return f'Name too short: {company}'
        
        # 0.5 Website menu items (ABOUT US, Contact, etc.)
        if company_lower in self.WEBSITE_MENU_ITEMS:
            return f'Website menu item: {company}'
        
        # 0.55 OEM garbage entities (article titles, headlines from OEM customer search)
        # GPT Audit Fix: brueckner_monforts.py produces junk like "Premium On Energy Efficiencies For Textile"
        # BUT: Skip this check if company has a real legal suffix (Ltd, S.A., GmbH etc.)
        if self.OEM_GARBAGE_PATTERNS.search(company) and not self.COMPANY_SUFFIXES.search(company):
            return f'OEM garbage entity: {company[:50]}'
        
        # 0.6 Country + company suffix pattern (Chile S.A., Brazil Ltd)
        country_suffix_pattern = re.compile(
            r'^(' + '|'.join(self.COUNTRY_NAMES) + r')\s*'
            r'(s\.?a\.?|ltd\.?|inc\.?|corp\.?|llc|gmbh|limited)?$',
            re.IGNORECASE
        )
        if country_suffix_pattern.match(company_lower.strip()):
            return f'Country name as company: {company}'
        
        # 1. Article fragments (the, of, in, a, an + noun)
        article_pattern = re.compile(
            r'^(the|of|in|a|an|to|for|with|from|by)\s+\w+$',
            re.IGNORECASE
        )
        if article_pattern.match(company):
            return f'Article fragment: {company}'
        
        # 2. Title/headline pattern
        if self.TITLE_PATTERNS.search(company):
            return f'Title pattern: {company[:50]}'
        
        # 3. Sentence fragment
        if self.SENTENCE_PATTERNS.search(company):
            return f'Sentence fragment: {company[:50]}'
        
        # 4. Person/role pattern
        if self.PERSON_PATTERNS.search(company):
            return f'Person/role pattern: {company[:50]}'
        
        # 4.5 V5: Garbage single words (common in oem_customer extractions)
        garbage_words = {
            'what', 'does', 'how', 'when', 'where', 'why', 'who', 'which',
            'upcoming', 'new', 'latest', 'best', 'top', 'modern', 'advanced',
            'using', 'used', 'uses', 'about', 'more', 'less', 'very', 'much',
            'also', 'even', 'just', 'only', 'some', 'any', 'all', 'each',
            'other', 'such', 'same', 'different', 'various', 'several',
            'ckner', 'nforts', 'antz', 'rtos', 'ntex',  # Truncated OEM names
        }
        if company_lower in garbage_words:
            return f'Garbage word: {company}'
        
        # 4.6 V5: Truncated company names (starts with lowercase or weird pattern)
        if company and not company[0].isupper() and len(company) > 2:
            # Likely a truncated name like "ckner Textile" from "Brückner Textile"
            return f'Truncated name (no capital start): {company[:30]}'
        
        # 4.7 V5: adjective + textile/machinery pattern without company suffix
        adj_noun_pattern = re.compile(
            r'^(upcoming|new|latest|modern|advanced|sustainable|technical|home|quality|german|turkish|brazilian)\s+'
            r'(textile|textiles|machinery|machine|machines|fabric|fabrics|finishing|dyeing)s?$',
            re.IGNORECASE
        )
        if adj_noun_pattern.match(company):
            return f'Adjective + generic noun: {company}'
        
        # 4.8 Stenter machine manufacturers - REJECT (they are OEMs, not customers)
        if company_lower in self.STENTER_OEM_MANUFACTURERS:
            return f'Stenter OEM manufacturer: {company}'
        
        # Check if company name contains OEM manufacturer name
        for oem in self.STENTER_OEM_MANUFACTURERS:
            if oem in company_lower and len(oem) > 4:  # Avoid false positives on short words
                return f'Contains OEM manufacturer name ({oem}): {company}'
        
        # 4.9 News/media sites - REJECT (not companies)
        if company_lower in self.NEWS_MEDIA_SITES:
            return f'News/media site: {company}'
        
        for news in self.NEWS_MEDIA_SITES:
            if news in company_lower:
                return f'Contains news site name ({news}): {company}'
        
        # 5. Machine/product name pattern
        if self.MACHINE_PATTERNS.search(company):
            return f'Machine/product name: {company[:50]}'
        
        # 5.5 GPT Audit Fix: Generic textile phrases ending with industry noun but no legal suffix
        # e.g., "Textile Manufacturers in Central America", "Global Textile", "Algerian textile"
        generic_textile_pattern = re.compile(
            r'^(\w+\s+){0,3}(textile|textiles|fabric|fabrics|finishing|dyeing|weaving|spinning|mills?|manufacturing)s?$',
            re.IGNORECASE
        )
        if generic_textile_pattern.match(company) and not self.COMPANY_SUFFIXES.search(company):
            # Check word count - 1-4 words ending with generic noun = not a company
            word_count = len(company.split())
            if word_count <= 4:
                return f'Generic textile phrase (no legal suffix): {company}'
        
        # 6. Single word + generic term
        words = company_lower.split()
        if len(words) == 1 and words[0] in self.GENERIC_TERMS:
            return f'Single generic term: {company}'
        
        # 7. All words are generic (up to 3 words)
        if 1 <= len(words) <= 3:
            clean_words = [w for w in words if w not in {'of', 'the', 'and', 'for', 'in', 'on', 'to'}]
            if all(w in self.GENERIC_TERMS for w in clean_words):
                return f'All generic terms: {company}'
        
        # 8. Check for marketplace/news domain in source
        if source_url:
            source_lower = source_url.lower()
            for domain in self.REJECT_DOMAINS:
                if domain in source_lower:
                    return f'Rejected domain: {domain}'
        
        # 9. Very long name (likely article title)
        if len(company) > 80:
            return f'Name too long ({len(company)} chars)'
        
        # 10. Contains multiple colons or special punctuation
        if company.count(':') > 1 or company.count('|') > 0:
            return f'Multiple colons/pipes: {company[:50]}'
        
        # 10. Starts with number (often article titles)
        if re.match(r'^\d+\s+', company) and 'no.' not in company_lower:
            return f'Starts with number: {company[:50]}'
        
        return None
    
    def _is_parts_supplier(self, lead: Dict) -> bool:
        """Check if lead is a parts supplier/trading company (not ideal customer)."""
        context = str(lead.get('context', '')).lower()
        company = str(lead.get('company', '')).lower()
        
        # Check company name
        if self.PARTS_SUPPLIER_PATTERNS.search(company):
            return True
        
        # Check context for strong parts supplier signals
        parts_signals = [
            'we have stock of',
            'spare parts stock',
            'largest stock of',
            'parts supplier',
            'machinery parts',
            'trading company',
            'import export',
            'reseller',
            'dealer',
        ]
        
        for signal in parts_signals:
            if signal in context:
                return True
        
        return False
    
    def normalize_country(self, country: str) -> str:
        """Normalize country name to standard English version."""
        if not country:
            return ''
        
        country_clean = str(country).strip().lower()
        
        # Handle NaN/None
        if country_clean in ('nan', 'none', 'null', ''):
            return ''
        
        # Look up in normalization map
        return self.COUNTRY_NORMALIZE.get(country_clean, country.strip())
    
    def _track_rejection(self, reason: str):
        """Track rejection reasons for analysis."""
        self.reject_count += 1
        self.grade_counts['REJECT'] += 1
        
        # Extract reason category
        category = reason.split(':')[0] if ':' in reason else reason
        self.rejection_reasons[category] = self.rejection_reasons.get(category, 0) + 1
    
    def process_leads(self, leads: List[Dict]) -> List[Dict]:
        """
        Process all leads and add quality grades.
        
        Returns only non-rejected leads with grade added.
        Applies country normalization and parts supplier flagging.
        """
        qualified = []
        parts_supplier_count = 0
        
        for lead in leads:
            # Normalize country first
            if lead.get('country'):
                lead['country'] = self.normalize_country(lead['country'])
            
            grade, reason = self.grade_entity(lead)
            
            if grade != 'REJECT':
                lead['entity_grade'] = grade
                lead['grade_reason'] = reason
                
                # Check and flag parts suppliers (don't reject, just flag)
                if self._is_parts_supplier(lead):
                    lead['is_parts_supplier'] = True
                    lead['entity_grade'] = 'C'  # Downgrade parts suppliers
                    lead['grade_reason'] = reason + '; Parts supplier flagged'
                    parts_supplier_count += 1
                else:
                    lead['is_parts_supplier'] = False
                
                qualified.append(lead)
            else:
                logger.debug(f"Rejected: {lead.get('company', 'Unknown')} - {reason}")
        
        logger.info(f"Entity Quality Gate: {len(leads)} -> {len(qualified)} leads")
        logger.info(f"Grades: A={self.grade_counts['A']}, B={self.grade_counts['B']}, "
                   f"C={self.grade_counts['C']}, REJECT={self.grade_counts['REJECT']}")
        logger.info(f"Parts suppliers flagged: {parts_supplier_count}")
        
        if self.rejection_reasons:
            logger.info("Rejection breakdown:")
            for reason, count in sorted(self.rejection_reasons.items(), key=lambda x: -x[1])[:5]:
                logger.info(f"  {reason}: {count}")
        
        return qualified
    
    def get_stats(self) -> Dict:
        """Return processing statistics."""
        return {
            'total_rejected': self.reject_count,
            'grade_counts': self.grade_counts.copy(),
            'rejection_reasons': self.rejection_reasons.copy()
        }


def apply_quality_gate_v2(leads: List[Dict]) -> List[Dict]:
    """Convenience function to apply v2 quality gate."""
    gate = EntityQualityGateV2()
    return gate.process_leads(leads)


if __name__ == '__main__':
    # Test with sample data
    test_leads = [
        {'company': 'ABC Tekstil A.Ş.', 'source_type': 'directory'},
        {'company': 'Stenter Machine: Types,', 'source_type': 'brave_search'},
        {'company': 'We are now fully supported in Turkey', 'source_type': 'brave_search'},
        {'company': 'Monforts Technologist for Dyeing', 'source_type': 'brave_search'},
        {'company': 'Brückner Stenter', 'source_type': 'brave_search'},
        {'company': 'Dyeing and Finishing', 'source_type': 'brave_search'},
        {'company': 'Aksa Akrilik Kimya A.Ş.', 'source_type': 'gots', 'website': 'aksa.com'},
        {'company': 'textile', 'source_type': 'brave_search'},
        {'company': 'Korteks Mensucat A.Ş.', 'source_type': 'oem_customer'},
    ]
    
    logging.basicConfig(level=logging.INFO)
    qualified = apply_quality_gate_v2(test_leads)
    
    print("\n=== QUALIFIED ===")
    for lead in qualified:
        print(f"  [{lead['entity_grade']}] {lead['company']}")
