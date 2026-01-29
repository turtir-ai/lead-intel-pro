#!/usr/bin/env python3
"""
Entity Quality Gate - Filter non-company entities before CRM
Implements skill: entity-quality-gate
"""

import re
import logging
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)


class EntityQualityGate:
    """
    Filters leads to reject non-company entities and grade real companies.
    
    Quality Grades:
    - A: High confidence (company suffix + website)
    - B: Medium confidence (2+ words + domain OR directory source)
    - C: Low confidence (needs manual review)
    - REJECT: Not a company (article, marketplace, generic term)
    """
    
    # Company suffixes indicating real entity
    COMPANY_SUFFIXES = [
        r'\b(gmbh|ltd|llc|inc|corp|sa\b|s\.a\.|ag\b|a\.g\.|kg\b|k\.g\.|',
        r'co\.|company|limited|plc|pty|bv\b|b\.v\.|nv\b|n\.v\.|',
        r'srl|s\.r\.l\.|spa|s\.p\.a\.|as\b|a\.s\.|oy|ab\b|',
        r'anonim|a\.ş\.|ltd\.şti\.|ltda|cia|industries|group|holdings)\b'
    ]
    SUFFIX_PATTERN = re.compile(''.join(COMPANY_SUFFIXES), re.IGNORECASE)
    
    # Domains to reject (marketplace/academic)
    REJECT_DOMAINS = {
        # Marketplaces
        'alibaba.com', 'aliexpress.com', 'indiamart.com', 'made-in-china.com',
        'globalsources.com', 'ec21.com', 'tradekey.com', 'dhgate.com',
        'exportersindia.com', 'tradeindia.com', 'go4worldbusiness.com',
        
        # Academic
        'sciencedirect.com', 'researchgate.net', 'academia.edu', 'springer.com',
        'wiley.com', 'tandfonline.com', 'mdpi.com', 'elsevier.com',
        'journals.sagepub.com', 'nature.com', 'ieee.org',
        
        # News/generic
        'wikipedia.org', 'youtube.com', 'facebook.com', 'linkedin.com',
        'twitter.com', 'instagram.com', 'pinterest.com',
        
        # PDF hosts
        'pdfhost.io', 'docdroid.net', 'scribd.com', 'slideshare.net'
    }
    
    # Generic terms that are NOT company names
    GENERIC_TERMS = {
        'manufacturer', 'manufacturing', 'textile', 'textiles', 'fabric', 'fabrics',
        'finishing', 'dyeing', 'bleaching', 'printing', 'processing',
        'machine', 'machinery', 'equipment', 'technology', 'process',
        'industry', 'industrial', 'production', 'products', 'product',
        'stand', 'booth', 'hall', 'exhibitor', 'exhibition',
        'service', 'services', 'solutions', 'systems', 'system',
        'global', 'international', 'world', 'worldwide',
        'supplier', 'suppliers', 'distributor', 'distributors',
        'unknown', 'other', 'various', 'multiple', 'general'
    }
    
    # Patterns indicating article/news (not company)
    ARTICLE_PATTERNS = [
        r'^how\s+to\b',
        r'^what\s+is\b', 
        r'^why\b',
        r'\bannounces\b',
        r'\breveals\b',
        r'\blaunch(?:es|ed)?\b',
        r'\bnew\s+(?:product|technology|method|process)\b',
        r'^the\s+(?:best|top|latest|new)\b',
        r'\bguide\s+to\b',
        r'\bintroduction\s+to\b',
    ]
    ARTICLE_PATTERN = re.compile('|'.join(ARTICLE_PATTERNS), re.IGNORECASE)
    
    # High confidence source types
    HIGH_CONFIDENCE_SOURCES = {
        'known_manufacturer', 'oem_customer', 'association_member',
        'gots', 'oekotex', 'fair_exhibitor', 'directory'
    }
    
    def __init__(self, config_path: Optional[Path] = None):
        self.config = self._load_config(config_path)
        self.reject_count = 0
        self.grade_counts = {'A': 0, 'B': 0, 'C': 0, 'REJECT': 0}
    
    def _load_config(self, config_path: Optional[Path]) -> dict:
        """Load additional blacklist patterns from config."""
        if config_path and config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    return yaml.safe_load(f) or {}
            except Exception as e:
                logger.warning(f"Failed to load entity blacklist config: {e}")
        return {}
    
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
        context = str(lead.get('context', ''))
        
        # Empty company name
        if not company or company.lower() in ('nan', 'none', ''):
            return 'REJECT', 'Empty company name'
        
        # Check rejection rules first
        reject_reason = self._check_rejection(company, source_url, context)
        if reject_reason:
            return 'REJECT', reject_reason
        
        # Now grade the entity
        grade_score = 0
        reasons = []
        
        # Company suffix check
        if self.SUFFIX_PATTERN.search(company):
            grade_score += 2
            reasons.append('Has company suffix')
        
        # Word count check (2+ words indicates real company)
        word_count = len(company.split())
        if word_count >= 2:
            grade_score += 1
            reasons.append(f'{word_count} words')
        elif word_count == 1 and len(company) < 4:
            grade_score -= 1
            reasons.append('Very short name')
        
        # High confidence source
        if source_type in self.HIGH_CONFIDENCE_SOURCES:
            grade_score += 2
            reasons.append(f'High confidence source: {source_type}')
        
        # Has website
        if website and website.lower() not in ('nan', 'none', '', '[]'):
            grade_score += 1
            reasons.append('Has website')
        
        # Determine grade
        if grade_score >= 3:
            grade = 'A'
        elif grade_score >= 1:
            grade = 'B'
        else:
            grade = 'C'
        
        return grade, '; '.join(reasons) if reasons else 'Default grade'
    
    def _check_rejection(self, company: str, source_url: str, context: str) -> Optional[str]:
        """
        Check if entity should be rejected.
        
        Returns:
            Rejection reason string, or None if not rejected
        """
        company_lower = company.lower().strip()
        
        # 1. Single word + generic term
        words = company_lower.split()
        if len(words) == 1 and words[0] in self.GENERIC_TERMS:
            return f'Single generic term: {company}'
        
        # 2. All words are generic
        if len(words) <= 3 and all(w in self.GENERIC_TERMS for w in words):
            return f'All generic terms: {company}'
        
        # 3. Check for marketplace/academic domain in source
        if source_url:
            source_lower = source_url.lower()
            for domain in self.REJECT_DOMAINS:
                if domain in source_lower:
                    return f'Rejected domain: {domain}'
        
        # 4. Article/news pattern in company name
        if self.ARTICLE_PATTERN.search(company):
            return f'Article pattern detected: {company}'
        
        # 5. Very long "company name" (likely a sentence/title)
        if len(company) > 100:
            return f'Name too long ({len(company)} chars), likely article title'
        
        # 6. Contains forbidden substrings
        forbidden = ['alibaba', 'indiamart', 'made-in-china', 'amazon', 'ebay']
        for f in forbidden:
            if f in company_lower:
                return f'Contains marketplace name: {f}'
        
        # 7. Looks like a URL
        if 'http' in company_lower or 'www.' in company_lower:
            return 'Company name contains URL'
        
        # 8. Starts with common article starters
        article_starters = ['the ', 'a ', 'an ', 'this ', 'that ', 'these ', 'those ']
        for starter in article_starters:
            if company_lower.startswith(starter) and len(words) > 4:
                return f'Starts with article word and too long: {company}'
        
        return None
    
    def filter_leads(self, leads: List[Dict]) -> List[Dict]:
        """
        Filter a list of leads, grading and rejecting as needed.
        
        Returns:
            List of leads with entity_quality and quality_reason fields added.
            Rejected leads are excluded.
        """
        filtered = []
        rejected_log = []
        
        for lead in leads:
            grade, reason = self.grade_entity(lead)
            self.grade_counts[grade] += 1
            
            if grade == 'REJECT':
                self.reject_count += 1
                rejected_log.append({
                    'company': lead.get('company', ''),
                    'source': lead.get('source_url', lead.get('source', '')),
                    'reason': reason
                })
                continue
            
            lead['entity_quality'] = grade
            lead['quality_reason'] = reason
            filtered.append(lead)
        
        # Log summary
        logger.info(f"Entity Quality Gate: {len(leads)} -> {len(filtered)} leads")
        logger.info(f"Grades: A={self.grade_counts['A']}, B={self.grade_counts['B']}, "
                   f"C={self.grade_counts['C']}, REJECT={self.grade_counts['REJECT']}")
        
        # Save rejected log
        self._save_rejected_log(rejected_log)
        
        return filtered
    
    def _save_rejected_log(self, rejected: List[Dict]):
        """Save rejected entities to log file."""
        if not rejected:
            return
        
        log_path = Path(__file__).parent.parent.parent / 'logs' / 'entity_rejected.log'
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(log_path, 'a', encoding='utf-8') as f:
                for item in rejected:
                    f.write(f"{item['company']} | {item['source']} | {item['reason']}\n")
        except Exception as e:
            logger.warning(f"Failed to write rejection log: {e}")
    
    def get_stats(self) -> Dict:
        """Get filtering statistics."""
        return {
            'total_rejected': self.reject_count,
            'grade_distribution': self.grade_counts.copy()
        }


# Standalone usage
if __name__ == '__main__':
    import pandas as pd
    
    logging.basicConfig(level=logging.INFO)
    
    base = Path(__file__).parent.parent.parent
    input_file = base / 'outputs' / 'crm' / 'targets_master.csv'
    
    if not input_file.exists():
        print(f"Input file not found: {input_file}")
        exit(1)
    
    df = pd.read_csv(input_file)
    leads = df.to_dict('records')
    
    gate = EntityQualityGate()
    filtered = gate.filter_leads(leads)
    
    print(f"\n{'='*60}")
    print("ENTITY QUALITY GATE RESULTS")
    print('='*60)
    print(f"Input: {len(leads)} leads")
    print(f"Output: {len(filtered)} leads")
    print(f"Rejected: {gate.reject_count}")
    print(f"\nGrade Distribution:")
    for grade, count in gate.grade_counts.items():
        pct = (count / len(leads) * 100) if leads else 0
        print(f"  {grade}: {count} ({pct:.1f}%)")
    
    # Save filtered output
    output_file = base / 'outputs' / 'crm' / 'targets_quality_filtered.csv'
    pd.DataFrame(filtered).to_csv(output_file, index=False)
    print(f"\nFiltered output saved to: {output_file}")
