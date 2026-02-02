"""
Network Sniffer - Phase 3: Advanced Discovery
Intercepts XHR/JSON responses from JavaScript-rendered pages
Extracts company data that's not visible in HTML
"""

import json
import logging
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, Page, Response
import time

logger = logging.getLogger(__name__)


class NetworkSniffer:
    """
    Extracts data from JavaScript-rendered pages by intercepting network requests
    
    Use cases:
    - global-trace-base.org: GOTS members loaded via XHR
    - Association directories with dynamic loading
    - Trade fair exhibitor lists loaded via API
    """
    
    def __init__(self, timeout: int = 30000, headless: bool = True):
        """
        Initialize network sniffer
        
        Args:
            timeout: Page load timeout in milliseconds
            headless: Run browser in headless mode
        """
        self.timeout = timeout
        self.headless = headless
        self.captured_responses = []
    
    def sniff_xhr_json(self, url: str, wait_for_idle: bool = True) -> List[Dict]:
        """
        Load page and capture all XHR/Fetch JSON responses
        
        Args:
            url: URL to load
            wait_for_idle: Wait for network to be idle before returning
            
        Returns:
            List of JSON responses captured
        """
        captured_data = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=self.headless)
                context = browser.new_context(
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                )
                page = context.new_page()
                
                # Set up response handler
                def handle_response(response: Response):
                    """Capture XHR/Fetch responses"""
                    try:
                        # Only capture XHR/Fetch requests
                        if response.request.resource_type in ["xhr", "fetch"]:
                            # Check if response is JSON
                            content_type = response.headers.get("content-type", "")
                            if "application/json" in content_type or "text/json" in content_type:
                                try:
                                    data = response.json()
                                    captured_data.append({
                                        'url': response.url,
                                        'status': response.status,
                                        'data': data
                                    })
                                    logger.debug(f"Captured JSON from {response.url}")
                                except Exception as e:
                                    logger.debug(f"Failed to parse JSON from {response.url}: {e}")
                    except Exception as e:
                        logger.debug(f"Error handling response: {e}")
                
                page.on("response", handle_response)
                
                # Navigate and wait
                if wait_for_idle:
                    page.goto(url, timeout=self.timeout, wait_until="networkidle")
                else:
                    page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
                    time.sleep(3)  # Wait a bit for XHR to complete
                
                browser.close()
                
        except Exception as e:
            logger.error(f"Network sniffing failed for {url}: {e}")
        
        logger.info(f"Captured {len(captured_data)} JSON responses from {url}")
        return captured_data
    
    def extract_companies_from_response(self, response_data: Dict, 
                                       company_keys: List[str] = None) -> List[Dict]:
        """
        Extract company data from JSON response
        
        Args:
            response_data: Captured JSON response
            company_keys: Keys to look for companies (e.g., ['companies', 'members', 'results'])
            
        Returns:
            List of company dicts
        """
        if company_keys is None:
            company_keys = [
                'companies', 'members', 'results', 'data', 'items',
                'records', 'suppliers', 'manufacturers', 'exhibitors'
            ]
        
        companies = []
        data = response_data.get('data', {})
        
        # Try each possible key
        for key in company_keys:
            if key in data:
                items = data[key]
                if isinstance(items, list):
                    companies.extend(items)
                    logger.info(f"Found {len(items)} companies under key '{key}'")
                    break
        
        # If no direct key, look deeper
        if not companies:
            companies = self._recursive_find_companies(data)
        
        return companies
    
    def _recursive_find_companies(self, obj, depth=0, max_depth=3) -> List[Dict]:
        """
        Recursively search for company arrays in nested JSON
        
        Args:
            obj: Object to search
            depth: Current depth
            max_depth: Maximum recursion depth
            
        Returns:
            List of potential company dicts
        """
        if depth > max_depth:
            return []
        
        companies = []
        
        if isinstance(obj, list):
            # If list contains dicts with company-like fields
            if obj and isinstance(obj[0], dict):
                first_item = obj[0]
                company_indicators = ['name', 'company', 'organization', 'email', 'website']
                if any(key in first_item for key in company_indicators):
                    return obj
        
        elif isinstance(obj, dict):
            for value in obj.values():
                if isinstance(value, (dict, list)):
                    companies.extend(self._recursive_find_companies(value, depth + 1, max_depth))
        
        return companies
    
    def sniff_and_extract(self, url: str, 
                         company_keys: List[str] = None,
                         min_companies: int = 5) -> List[Dict]:
        """
        Complete workflow: sniff network and extract companies
        
        Args:
            url: URL to scrape
            company_keys: Keys to look for company arrays
            min_companies: Minimum companies to consider success
            
        Returns:
            List of company dicts
        """
        logger.info(f"Network sniffing: {url}")
        
        # Capture responses
        responses = self.sniff_xhr_json(url)
        
        if not responses:
            logger.warning(f"No JSON responses captured from {url}")
            return []
        
        # Extract companies from all responses
        all_companies = []
        for response in responses:
            companies = self.extract_companies_from_response(response, company_keys)
            all_companies.extend(companies)
        
        # Deduplicate
        unique_companies = self._deduplicate_companies(all_companies)
        
        logger.info(f"Extracted {len(unique_companies)} unique companies from {url}")
        
        if len(unique_companies) < min_companies:
            logger.warning(f"Only found {len(unique_companies)} companies (expected >= {min_companies})")
        
        return unique_companies
    
    def _deduplicate_companies(self, companies: List[Dict]) -> List[Dict]:
        """
        Remove duplicate companies based on name/email
        
        Args:
            companies: List of company dicts
            
        Returns:
            Deduplicated list
        """
        seen = set()
        unique = []
        
        for company in companies:
            # Create key from name or email
            name = company.get('name') or company.get('company') or company.get('organization', '')
            email = company.get('email', '')
            
            key = (name.lower().strip() if name else '', email.lower().strip() if email else '')
            
            if key not in seen and key != ('', ''):
                seen.add(key)
                unique.append(company)
        
        return unique
    
    def normalize_company_data(self, raw_company: Dict, source_url: str) -> Dict:
        """
        Normalize extracted company data to standard format
        
        Args:
            raw_company: Raw company dict from JSON
            source_url: Source URL
            
        Returns:
            Normalized company dict
        """
        # Try to extract standard fields
        company = raw_company.get('name') or raw_company.get('company') or raw_company.get('organization', '')
        email = raw_company.get('email') or raw_company.get('contact_email', '')
        website = raw_company.get('website') or raw_company.get('url') or raw_company.get('web', '')
        phone = raw_company.get('phone') or raw_company.get('telephone') or raw_company.get('tel', '')
        address = raw_company.get('address') or raw_company.get('location') or raw_company.get('city', '')
        country = raw_company.get('country') or raw_company.get('nation', '')
        
        return {
            'company': company,
            'email': email,
            'website': website,
            'phone': phone,
            'address': address,
            'country': country,
            'source_type': 'network_sniff',
            'source_url': source_url,
            'raw_data': raw_company  # Keep original for debugging
        }


class GOTSDirectorySniffer(NetworkSniffer):
    """
    Specialized sniffer for global-trace-base.org (GOTS directory)
    """
    
    BASE_URL = "https://global-trace-base.org"
    
    def harvest_gots_members(self) -> List[Dict]:
        """
        Extract GOTS certified companies via network sniffing
        
        Returns:
            List of company dicts
        """
        logger.info("Harvesting GOTS members via network sniffing...")
        
        # GOTS directory URL (adjust based on actual site structure)
        url = f"{self.BASE_URL}/search"
        
        # Sniff and extract
        raw_companies = self.sniff_and_extract(
            url=url,
            company_keys=['results', 'companies', 'members', 'suppliers'],
            min_companies=50
        )
        
        # Normalize to standard format
        normalized = [
            self.normalize_company_data(raw, url)
            for raw in raw_companies
        ]
        
        logger.info(f"GOTS Network Sniffing: {len(normalized)} companies extracted")
        return normalized
