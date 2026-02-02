"""
Brave Search Integration - Phase 2: Pipeline Enhancement
Handles website discovery and Stenter Customer Evidence (SCE) search
"""

import os
import requests
from typing import List, Dict, Optional
from time import sleep, time
import logging

logger = logging.getLogger(__name__)


class BraveSearchClient:
    """
    Wrapper for Brave Search API
    Rate limit: 15 calls/min (1 call per 4 seconds)
    """
    
    BASE_URL = "https://api.search.brave.com/res/v1/web/search"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Brave client
        
        Args:
            api_key: Brave API key (reads from env if not provided)
        """
        self.api_key = api_key or os.getenv('Brave_API_KEY') or os.getenv('BRAVE_API_KEY')
        if not self.api_key:
            logger.warning("No Brave API key found. Set Brave_API_KEY or BRAVE_API_KEY env variable.")
        
        self.calls_made = 0
        self.rate_limit = 15  # calls per minute
        self.last_call_time = 0
        self.min_delay = 4.0  # seconds between calls (60/15 = 4)
    
    def _rate_limit_check(self):
        """Respect API rate limits with delays"""
        # Calculate time since last call
        elapsed = time() - self.last_call_time
        
        # If less than min_delay, wait
        if elapsed < self.min_delay:
            wait_time = self.min_delay - elapsed
            logger.debug(f"Rate limiting: waiting {wait_time:.1f}s")
            sleep(wait_time)
        
        self.last_call_time = time()
    
    def search(self, query: str, count: int = 5) -> List[Dict]:
        """
        Execute search query
        
        Args:
            query: Search string (e.g., "ABC Textiles Pakistan stenter")
            count: Number of results (max 20)
        
        Returns:
            List of result dicts with keys: title, url, description
        """
        if not self.api_key:
            logger.warning("No API key - skipping search")
            return []
        
        self._rate_limit_check()
        
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": self.api_key
        }
        
        params = {
            "q": query,
            "count": min(count, 20)
        }
        
        try:
            response = requests.get(self.BASE_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            self.calls_made += 1
            
            data = response.json()
            results = data.get('web', {}).get('results', [])
            
            logger.debug(f"Brave search: '{query}' returned {len(results)} results")
            return results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Brave API error: {e}")
            return []
    
    def discover_website(self, company_name: str, country: str = "") -> Optional[str]:
        """
        Find company's official website
        
        Strategy:
        1. Search "{company_name} {country} official website"
        2. Filter out social media/directories
        3. Return first valid result
        
        Args:
            company_name: Company name
            country: Country (optional, helps with disambiguation)
            
        Returns:
            URL or None
        """
        # Build query
        country_str = f" {country}" if country else ""
        query = f'"{company_name}"{country_str} official website'
        
        results = self.search(query, count=5)
        
        # Domain filters - exclude these
        invalid_domains = [
            'linkedin.com', 'facebook.com', 'instagram.com', 
            'wikipedia.org', 'indiamart.com', 'alibaba.com',
            'made-in-china.com', 'tradekey.com', 'youtube.com',
            'twitter.com', 'x.com'
        ]
        
        for result in results:
            url = result.get('url', '')
            
            # Skip invalid domains
            if any(domain in url.lower() for domain in invalid_domains):
                continue
            
            # Found valid website
            logger.info(f"Discovered website for {company_name}: {url}")
            return url
        
        logger.debug(f"No valid website found for {company_name}")
        return None
    
    def find_evidence(self, company_name: str, country: str = "", 
                     website: str = "") -> Dict:
        """
        Search for Stenter Customer Evidence (SCE)
        
        Query pattern: "{company} {country} stenter OR stenters OR textile finishing"
        
        Args:
            company_name: Company name
            country: Country (optional)
            website: Known website (optional, helps narrow search)
            
        Returns:
            {
                'has_evidence': bool,
                'evidence_type': str,
                'evidence_url': str,
                'evidence_text': str,
                'confidence': str  # 'strong', 'medium', 'weak'
            }
        """
        # Machine keywords to search for
        machine_keywords = [
            "stenter", "stenters", "heat setting", 
            "textile finishing", "fabric processing",
            "finishing line", "continuous processing"
        ]
        
        # Build query
        country_str = f" {country}" if country else ""
        site_filter = f" site:{website.replace('http://', '').replace('https://', '').split('/')[0]}" if website else ""
        
        query = f'"{company_name}"{country_str}{site_filter} {" OR ".join(machine_keywords)}'
        
        results = self.search(query, count=10)
        
        # Evidence keywords with confidence levels
        strong_evidence = [
            "installed stenter", "stenter machine", "stenter parts",
            "heat setting equipment", "stenters installed",
            "finishing line", "brückner stenter", "monforts stenter"
        ]
        
        medium_evidence = [
            "textile finishing", "fabric processing", "dyeing and finishing",
            "continuous processing", "heat treatment", "fabric treatment"
        ]
        
        # Check results for evidence
        for result in results:
            text = f"{result.get('title', '')} {result.get('description', '')}".lower()
            
            # Check strong evidence
            for evidence_kw in strong_evidence:
                if evidence_kw in text:
                    return {
                        'has_evidence': True,
                        'evidence_type': evidence_kw,
                        'evidence_url': result.get('url'),
                        'evidence_text': result.get('description', '')[:200],
                        'confidence': 'strong'
                    }
            
            # Check medium evidence
            for evidence_kw in medium_evidence:
                if evidence_kw in text:
                    return {
                        'has_evidence': True,
                        'evidence_type': evidence_kw,
                        'evidence_url': result.get('url'),
                        'evidence_text': result.get('description', '')[:200],
                        'confidence': 'medium'
                    }
        
        return {
            'has_evidence': False,
            'confidence': 'none'
        }
    
    def batch_discover(self, leads: List[Dict]) -> List[Dict]:
        """
        Batch website discovery for multiple leads
        
        Args:
            leads: List of lead dicts with 'company' and 'country'
            
        Returns:
            Updated leads with 'website' filled
        """
        discovered = 0
        skipped_has_website = 0
        skipped_no_company = 0
        failed = 0
        
        for i, lead in enumerate(leads):
            # Handle pandas NaN values
            website = lead.get('website')
            if website and str(website) != 'nan':
                skipped_has_website += 1
                continue
            
            # Skip if marked as needs_discovery=False
            if lead.get('needs_discovery') == False:
                continue
            
            company = lead.get('company') or lead.get('company_name', '')
            country = lead.get('country', '')
            
            # Handle NaN in company/country
            if not company or str(company) == 'nan':
                skipped_no_company += 1
                continue
            
            if str(country) == 'nan':
                country = ''
            
            # Log progress every 10 items
            if (i + 1) % 10 == 0:
                logger.info(f"Discovery progress: {i+1}/{len(leads)} processed, {discovered} found")
            
            # Discover website
            try:
                website = self.discover_website(company, country)
                
                if website:
                    lead['website'] = website
                    lead['website_source'] = 'brave_discovery'
                    discovered += 1
                else:
                    failed += 1
            except Exception as e:
                logger.warning(f"Discovery failed for {company}: {e}")
                failed += 1
        
        logger.info(f"Batch discovery: {discovered}/{len(leads)} websites found, "
                   f"{skipped_has_website} already had website, "
                   f"{skipped_no_company} had no company name, "
                   f"{failed} failed")
        return leads
    
    def batch_evidence_search(self, leads: List[Dict]) -> List[Dict]:
        """
        Batch evidence search for multiple leads
        
        Args:
            leads: List of lead dicts
            
        Returns:
            Updated leads with SCE evidence fields
        """
        evidence_found = 0
        skipped_no_company = 0
        
        for i, lead in enumerate(leads):
            company = lead.get('company') or lead.get('company_name', '')
            country = lead.get('country', '')
            website = lead.get('website', '')
            
            # Handle pandas NaN
            if not company or str(company) == 'nan':
                skipped_no_company += 1
                continue
            
            if str(country) == 'nan':
                country = ''
            
            if str(website) == 'nan':
                website = ''
            
            # Log progress every 10 items
            if (i + 1) % 10 == 0:
                logger.info(f"Evidence search progress: {i+1}/{len(leads)} processed, {evidence_found} found")
            
            # Search for evidence
            try:
                evidence = self.find_evidence(company, country, website)
                
                # Add evidence fields to lead
                lead['sce_has_evidence'] = evidence.get('has_evidence', False)
                lead['sce_evidence_type'] = evidence.get('evidence_type', '')
                lead['sce_evidence_url'] = evidence.get('evidence_url', '')
                lead['sce_evidence_text'] = evidence.get('evidence_text', '')
                lead['sce_confidence'] = evidence.get('confidence', 'none')
                
                if evidence.get('has_evidence'):
                    evidence_found += 1
            except Exception as e:
                logger.warning(f"Evidence search failed for {company}: {e}")
                lead['sce_has_evidence'] = False
                lead['sce_confidence'] = 'error'
        
        logger.info(f"Batch evidence: {evidence_found}/{len(leads)} leads have SCE, "
                   f"{skipped_no_company} skipped (no company name)")
        return leads
    
    def get_stats(self) -> Dict:
        """Get API usage statistics"""
        return {
            'calls_made': self.calls_made,
            'rate_limit': self.rate_limit,
            'api_configured': bool(self.api_key)
        }
