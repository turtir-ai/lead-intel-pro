"""
Phase 3: LATAM source expansion collectors
Targets Brazil, Colombia, Argentina, Peru, Mexico textile associations
"""

import requests
from typing import List, Dict, Optional
from datetime import datetime
from bs4 import BeautifulSoup
import time

from src.utils.logger import get_logger
from src.utils.http_client import HttpClient
from src.utils.evidence import record_evidence

logger = get_logger(__name__)


def _save_html_cache(url: str, html: str) -> str:
    """Save HTML to cache and return content hash"""
    import hashlib
    content_hash = hashlib.sha256(html.encode()).hexdigest()
    return content_hash


class LATAMSourceCollector:
    """Base class for LATAM source collectors"""
    
    def __init__(self, evidence_path: str = "outputs/evidence/evidence_log.csv"):
        self.evidence_path = evidence_path
        self.collected_companies = []
        self.http_client = HttpClient()
    
    def normalize_company(self, raw_data: Dict, source_name: str, country: str) -> Dict:
        """
        Normalize company data to standard format
        
        Args:
            raw_data: Raw company dict from source
            source_name: Name of source (e.g., 'ABIT', 'Inexmoda')
            country: Country code
            
        Returns:
            Normalized company dict
        """
        return {
            'company': raw_data.get('company', '').strip(),
            'country': country,
            'website': raw_data.get('website', '').strip(),
            'email': raw_data.get('email', '').strip(),
            'phone': raw_data.get('phone', '').strip(),
            'address': raw_data.get('address', '').strip(),
            'context': f"Member of {source_name}",
            'source_type': 'latam_association',
            'source_name': source_name,
            'collected_at': datetime.utcnow().isoformat()
        }
    
    def record_source_evidence(self, url: str, title: str, snippet: str, content_hash: str):
        """Record evidence for collected source"""
        record_evidence(
            self.evidence_path,
            {
                'source_type': 'latam_association',
                'source_name': self.__class__.__name__,
                'url': url,
                'title': title,
                'snippet': snippet[:400],
                'content_hash': content_hash,
                'fetched_at': datetime.utcnow().isoformat()
            }
        )


class AbitCollector(LATAMSourceCollector):
    """
    Brazil - ABIT (Associação Brasileira da Indústria Têxtil)
    Brazilian Textile and Apparel Industry Association
    """
    
    BASE_URL = "https://www.abit.org.br"
    
    def collect(self) -> List[Dict]:
        """
        Collect ABIT member companies
        
        Strategy:
        1. Scrape member directory page
        2. Extract company listings
        3. Follow pagination if present
        
        Returns:
            List of company dicts
        """
        logger.info("Collecting companies from ABIT (Brazil)")
        companies = []
        
        # ABIT member directory URL
        member_url = f"{self.BASE_URL}/associados"
        
        try:
            html = self.http_client.get(member_url)
            if not html:
                logger.warning("No HTML returned from ABIT")
                return companies
            
            # Save to cache
            content_hash = _save_html_cache(member_url, html)
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find company listings (adjust selectors based on actual site structure)
            company_elements = soup.find_all(['div', 'li'], class_=lambda x: x and ('member' in x.lower() or 'associado' in x.lower()))
            
            if not company_elements:
                # Fallback: Look for any links or text blocks that might be companies
                company_elements = soup.find_all(['a', 'h3', 'h4'], href=True) or soup.find_all(['div'], class_='company')
            
            logger.info(f"Found {len(company_elements)} potential company elements")
            
            for element in company_elements:
                company_data = self._parse_company_element(element)
                if company_data and company_data.get('company'):
                    normalized = self.normalize_company(company_data, 'ABIT', 'Brazil')
                    companies.append(normalized)
            
            # Record evidence
            self.record_source_evidence(
                member_url,
                "ABIT Member Directory",
                f"Collected {len(companies)} companies from Brazilian textile association",
                content_hash
            )
            
        except Exception as e:
            logger.error(f"Error collecting from ABIT: {e}")
        
        logger.info(f"Collected {len(companies)} companies from ABIT")
        return companies
    
    def _parse_company_element(self, element) -> Optional[Dict]:
        """Parse company data from HTML element"""
        try:
            # Extract company name
            company = element.get_text(strip=True)
            
            # Extract website if present
            website = ''
            link = element.find('a', href=True)
            if link:
                href = link.get('href', '')
                if href and not href.startswith('#'):
                    website = href if href.startswith('http') else f"{self.BASE_URL}{href}"
            
            # Extract contact info from nested elements
            email = ''
            phone = ''
            
            email_elem = element.find(['a', 'span'], href=lambda x: x and 'mailto:' in x)
            if email_elem:
                email = email_elem.get('href', '').replace('mailto:', '')
            
            phone_elem = element.find(['span', 'a'], href=lambda x: x and 'tel:' in x)
            if phone_elem:
                phone = phone_elem.get_text(strip=True)
            
            return {
                'company': company,
                'website': website,
                'email': email,
                'phone': phone
            }
        
        except Exception as e:
            logger.debug(f"Error parsing company element: {e}")
            return None


class InexmodaCollector(LATAMSourceCollector):
    """
    Colombia - Inexmoda
    Colombian fashion and textile association
    """
    
    BASE_URL = "https://www.inexmoda.org.co"
    
    def collect(self) -> List[Dict]:
        """Collect Inexmoda member companies"""
        logger.info("Collecting companies from Inexmoda (Colombia)")
        companies = []
        
        member_url = f"{self.BASE_URL}/afiliados"
        
        try:
            html = self.http_client.get(member_url)
            if not html:
                return companies
            
            content_hash = _save_html_cache(member_url, html)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Parse company listings
            company_elements = soup.find_all(['div', 'li'], class_=lambda x: x and ('afiliado' in x.lower() or 'member' in x.lower()))
            
            for element in company_elements:
                company_data = self._parse_company_element(element)
                if company_data and company_data.get('company'):
                    normalized = self.normalize_company(company_data, 'Inexmoda', 'Colombia')
                    companies.append(normalized)
            
            self.record_source_evidence(
                member_url,
                "Inexmoda Affiliates Directory",
                f"Collected {len(companies)} companies from Colombian fashion association",
                content_hash
            )
            
        except Exception as e:
            logger.error(f"Error collecting from Inexmoda: {e}")
        
        logger.info(f"Collected {len(companies)} companies from Inexmoda")
        return companies
    
    def _parse_company_element(self, element) -> Optional[Dict]:
        """Parse company data from HTML element"""
        try:
            company = element.find(['h3', 'h4', 'a']).get_text(strip=True)
            
            website = ''
            link = element.find('a', href=True)
            if link:
                website = link.get('href', '')
            
            return {'company': company, 'website': website}
        
        except Exception as e:
            return None


class FITACollector(LATAMSourceCollector):
    """
    Argentina - FITA (Fundación Industrial Textil Argentina)
    Argentine Textile Industrial Foundation
    """
    
    BASE_URL = "https://www.fundacionfita.org.ar"
    
    def collect(self) -> List[Dict]:
        """Collect FITA member companies"""
        logger.info("Collecting companies from FITA (Argentina)")
        companies = []
        
        member_url = f"{self.BASE_URL}/empresas-asociadas"
        
        try:
            html = self.http_client.get(member_url)
            if not html:
                return companies
            
            content_hash = _save_html_cache(member_url, html)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Parse company listings
            company_elements = soup.find_all(['div', 'li'], class_=lambda x: x and 'empresa' in x.lower())
            
            for element in company_elements:
                company_data = self._parse_company_element(element)
                if company_data and company_data.get('company'):
                    normalized = self.normalize_company(company_data, 'FITA', 'Argentina')
                    companies.append(normalized)
            
            self.record_source_evidence(
                member_url,
                "FITA Member Companies",
                f"Collected {len(companies)} companies from Argentine textile foundation",
                content_hash
            )
            
        except Exception as e:
            logger.error(f"Error collecting from FITA: {e}")
        
        logger.info(f"Collected {len(companies)} companies from FITA")
        return companies
    
    def _parse_company_element(self, element) -> Optional[Dict]:
        """Parse company data from HTML element"""
        try:
            company = element.get_text(strip=True)
            website = element.find('a', href=True)
            
            return {
                'company': company,
                'website': website.get('href', '') if website else ''
            }
        
        except Exception as e:
            return None


class ComiteTextilCollector(LATAMSourceCollector):
    """
    Peru - Comité Textil SNI
    Peruvian Textile Committee (part of National Industries Society)
    """
    
    BASE_URL = "https://www.sni.org.pe"
    
    def collect(self) -> List[Dict]:
        """Collect Comite Textil member companies"""
        logger.info("Collecting companies from Comité Textil SNI (Peru)")
        companies = []
        
        member_url = f"{self.BASE_URL}/comite-textil/empresas"
        
        try:
            html = self.http_client.get(member_url)
            if not html:
                return companies
            
            content_hash = _save_html_cache(member_url, html)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Parse company listings
            company_elements = soup.find_all(['div', 'li'], class_=lambda x: x and 'empresa' in x.lower())
            
            for element in company_elements:
                company_data = self._parse_company_element(element)
                if company_data and company_data.get('company'):
                    normalized = self.normalize_company(company_data, 'Comité Textil SNI', 'Peru')
                    companies.append(normalized)
            
            self.record_source_evidence(
                member_url,
                "Comité Textil Member Companies",
                f"Collected {len(companies)} companies from Peruvian textile committee",
                content_hash
            )
            
        except Exception as e:
            logger.error(f"Error collecting from Comité Textil: {e}")
        
        logger.info(f"Collected {len(companies)} companies from Comité Textil")
        return companies
    
    def _parse_company_element(self, element) -> Optional[Dict]:
        """Parse company data from HTML element"""
        try:
            company = element.get_text(strip=True)
            website = element.find('a', href=True)
            
            return {
                'company': company,
                'website': website.get('href', '') if website else ''
            }
        
        except Exception as e:
            return None


class LATAMSourcesOrchestrator:
    """Orchestrates collection from all LATAM sources"""
    
    def __init__(self, evidence_path: str = "outputs/evidence/evidence_log.csv"):
        self.collectors = [
            AbitCollector(evidence_path),
            InexmodaCollector(evidence_path),
            FITACollector(evidence_path),
            ComiteTextilCollector(evidence_path)
        ]
    
    def collect_all(self, delay_between_sources: float = 2.0) -> List[Dict]:
        """
        Collect from all LATAM sources with rate limiting
        
        Args:
            delay_between_sources: Seconds to wait between sources
            
        Returns:
            Combined list of companies from all sources
        """
        logger.info("Starting LATAM sources collection")
        all_companies = []
        
        for collector in self.collectors:
            try:
                companies = collector.collect()
                all_companies.extend(companies)
                
                logger.info(f"{collector.__class__.__name__}: {len(companies)} companies")
                
                # Rate limiting
                time.sleep(delay_between_sources)
                
            except Exception as e:
                logger.error(f"Error in {collector.__class__.__name__}: {e}")
        
        logger.info(f"Total LATAM companies collected: {len(all_companies)}")
        return all_companies
