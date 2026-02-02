#!/usr/bin/env python3
"""
Lead Schema - Pydantic validation for leads
GPT V3 Fix: Ensures data quality at collection time

Rules:
- country: MUST be a country name, NOT a URL
- website: MUST be a valid URL, NOT an email
- emails: MUST be list of valid emails
- company: MUST pass noise filter
"""

import re
from typing import List, Optional
from pydantic import BaseModel, field_validator, model_validator
from datetime import datetime


class LeadSchema(BaseModel):
    """Canonical lead schema with validation."""
    
    company: str
    country: Optional[str] = None
    context: Optional[str] = None
    source_url: Optional[str] = None
    source_type: Optional[str] = None
    source_name: Optional[str] = None
    website: Optional[str] = None
    emails: List[str] = []
    phones: List[str] = []
    address: Optional[str] = None
    city: Optional[str] = None
    contact_name: Optional[str] = None
    certification: Optional[str] = None
    harvested_at: Optional[str] = None
    
    # NOISE PATTERNS - reject these as company names
    NOISE_PATTERNS = re.compile(
        r'^(view basket|new expertise|energy|yarn|clients|home_|'
        r'istanbul event|sign in|menu|header|footer|nav|'
        r'cookie|privacy policy|terms|about us|contact us|'
        r'read more|learn more|click here|subscribe|newsletter|'
        r'page \d|section|slide|tab|button|link|'
        r'loading|please wait|error|404|403|'
        r'search results|no results|empty|undefined|null|'
        r'textile world|textile machinery|textile industry|'
        r'view all|see more|show more|load more)$',
        re.IGNORECASE
    )
    
    # URL pattern for detection
    URL_PATTERN = re.compile(r'https?://|www\.|\.com|\.org|\.net|\.edu|\.gov', re.IGNORECASE)
    
    # Email pattern
    EMAIL_PATTERN = re.compile(r'@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    
    # Country names (valid values for country field)
    VALID_COUNTRIES = {
        'turkey', 'brazil', 'argentina', 'peru', 'colombia', 'ecuador', 'chile',
        'egypt', 'morocco', 'tunisia', 'algeria', 'libya',
        'pakistan', 'india', 'bangladesh', 'vietnam', 'indonesia', 'china',
        'germany', 'italy', 'spain', 'portugal', 'france', 'uk', 'usa',
        'mexico', 'uruguay', 'paraguay', 'bolivia', 'venezuela',
        'south africa', 'kenya', 'nigeria', 'ethiopia', 'tanzania',
        'thailand', 'malaysia', 'philippines', 'sri lanka', 'nepal',
        'türkiye', 'brasil', 'mısır', 'fas', 'tunus', 'hindistan',
    }
    
    @field_validator('company')
    @classmethod
    def validate_company(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError('Company name cannot be empty')
        
        v = v.strip()
        
        # Reject noise patterns
        if cls.NOISE_PATTERNS.match(v):
            raise ValueError(f'Noise pattern detected: {v}')
        
        # Reject too short names
        if len(v) < 3:
            raise ValueError(f'Company name too short: {v}')
        
        # Reject names that are just generic terms
        generic_only = {'textile', 'textiles', 'fabric', 'fabrics', 'yarn', 
                       'energy', 'machine', 'machines', 'equipment', 'parts'}
        if v.lower() in generic_only:
            raise ValueError(f'Generic term only: {v}')
        
        # Reject if it's a URL (schema mapping error)
        if cls.URL_PATTERN.search(v):
            raise ValueError(f'Company contains URL: {v}')
        
        # Reject if it's an email
        if cls.EMAIL_PATTERN.search(v):
            raise ValueError(f'Company contains email: {v}')
        
        return v
    
    @field_validator('country')
    @classmethod
    def validate_country(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return None
        
        v = v.strip()
        
        # CRITICAL: country CANNOT be a URL (schema mapping bug from OEKO-TEX)
        if cls.URL_PATTERN.search(v):
            raise ValueError(f'Country is a URL, not a country name: {v}')
        
        # CRITICAL: country CANNOT be an email
        if cls.EMAIL_PATTERN.search(v):
            raise ValueError(f'Country is an email, not a country name: {v}')
        
        # Normalize common country names
        country_normalize = {
            'türkiye': 'Turkey', 'turkiye': 'Turkey',
            'brasil': 'Brazil', 'brezilya': 'Brazil',
            'mısır': 'Egypt', 'misir': 'Egypt',
            'fas': 'Morocco', 'tunus': 'Tunisia',
            'hindistan': 'India', 'çin': 'China',
            'bangladeş': 'Bangladesh', 'arjantin': 'Argentina',
            'kolombiya': 'Colombia', 'şili': 'Chile',
            'ekvador': 'Ecuador', 'meksika': 'Mexico',
        }
        
        normalized = country_normalize.get(v.lower(), v)
        return normalized
    
    @field_validator('website')
    @classmethod
    def validate_website(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return None
        
        v = v.strip()
        
        # CRITICAL: website CANNOT be an email
        if '@' in v and not v.startswith('http'):
            raise ValueError(f'Website is an email: {v}')
        
        # Must be a valid URL or domain
        if v and not v.startswith('http') and not v.startswith('www.'):
            # Try to detect if it's a domain
            if '.' in v and not '@' in v:
                v = f'https://{v}'
            else:
                return None  # Not a valid website
        
        return v
    
    @field_validator('emails', mode='before')
    @classmethod
    def validate_emails(cls, v) -> List[str]:
        if not v:
            return []
        
        if isinstance(v, str):
            # Parse string representation of list
            if v.startswith('[') and v.endswith(']'):
                import ast
                try:
                    v = ast.literal_eval(v)
                except:
                    v = [v]
            else:
                v = [v]
        
        # Filter valid emails
        valid_emails = []
        for email in v:
            email = str(email).strip()
            if cls.EMAIL_PATTERN.search(email):
                valid_emails.append(email)
        
        return valid_emails
    
    @model_validator(mode='after')
    def check_consistency(self):
        """Cross-field validation."""
        # If website looks like source_url (OEKO-TEX profile), swap or clear
        if self.website and 'oeko-tex.com' in self.website:
            # This is the source URL, not the company website
            if not self.source_url:
                self.source_url = self.website
            self.website = None
        
        return self


def validate_lead(lead_dict: dict, strict: bool = False) -> tuple[dict, list[str]]:
    """
    Validate a lead dictionary against the schema.
    
    Args:
        lead_dict: Raw lead dictionary
        strict: If True, raise on errors. If False, return errors list.
    
    Returns:
        Tuple of (validated_dict or original, list of error messages)
    """
    errors = []
    
    try:
        validated = LeadSchema(**lead_dict)
        return validated.model_dump(), errors
    except Exception as e:
        if strict:
            raise
        errors.append(str(e))
        return lead_dict, errors


def validate_leads_batch(leads: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """
    Validate a batch of leads.
    
    Returns:
        Tuple of (valid_leads, invalid_leads, stats)
    """
    valid = []
    invalid = []
    stats = {
        'total': len(leads),
        'valid': 0,
        'invalid': 0,
        'error_types': {}
    }
    
    for lead in leads:
        validated, errors = validate_lead(lead)
        if errors:
            lead['_validation_errors'] = errors
            invalid.append(lead)
            stats['invalid'] += 1
            
            # Track error types
            for err in errors:
                err_type = err.split(':')[0] if ':' in err else err[:50]
                stats['error_types'][err_type] = stats['error_types'].get(err_type, 0) + 1
        else:
            valid.append(validated)
            stats['valid'] += 1
    
    return valid, invalid, stats


if __name__ == '__main__':
    # Test cases
    test_cases = [
        # Valid lead
        {
            'company': 'ABC Tekstil A.Ş.',
            'country': 'Turkey',
            'website': 'https://abc-tekstil.com',
            'emails': ['info@abc.com'],
            'source_type': 'gots'
        },
        # Invalid: country is URL (OEKO-TEX bug)
        {
            'company': 'Egyptian Fibers Company',
            'country': 'https://services.oeko-tex.com/profile/123',
            'website': 'info@efco.com',  # This is email, not website
        },
        # Invalid: noise company name
        {
            'company': 'View basket',
            'country': 'Brazil',
        },
        # Invalid: generic term only
        {
            'company': 'Energy',
            'country': 'Egypt',
        },
    ]
    
    for i, test in enumerate(test_cases):
        validated, errors = validate_lead(test)
        print(f"\nTest {i+1}: {test.get('company', 'N/A')}")
        if errors:
            print(f"  ❌ INVALID: {errors}")
        else:
            print(f"  ✅ VALID: {validated.get('company')}")
