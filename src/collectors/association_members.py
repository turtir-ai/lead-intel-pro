#!/usr/bin/env python3
"""
Association Members Harvester - Collect members from textile associations
Based on project_v4.md sources_catalog.yaml

Sources:
- EURATEX national associations (20+ countries)
- ITMF members (global)
- IVGT Germany (finishing/veredlung)
- Swiss Textiles
- ATP Portugal
- AMITH Morocco
"""

import re
import logging
import time
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class Association:
    """Textile association info."""
    id: str
    name: str
    country: str
    url: str
    member_directory_url: Optional[str] = None


@dataclass
class Member:
    """Association member."""
    company: str
    country: str
    website: Optional[str] = None
    city: Optional[str] = None
    source_association: str = ""
    evidence_url: str = ""


class AssociationMembersHarvester:
    """
    Harvests members from textile industry associations.
    
    Strategy:
    1. Start with umbrella associations (EURATEX, ITMF)
    2. Extract national association links
    3. Crawl each national association's member directory
    4. Extract company names, websites, countries
    """
    
    # EURATEX National Associations (from fetch_webpage results)
    EURATEX_MEMBERS = [
        Association('at_wko', 'WKO Austria', 'Austria', 'https://www.tbsl.at'),
        Association('be_creamoda', 'Creamoda', 'Belgium', 'https://www.creamoda.be'),
        Association('be_fedustria', 'Fedustria', 'Belgium', 'https://www.fedustria.be'),
        Association('de_textilmode', 'textil+mode', 'Germany', 'https://textil-mode.de'),
        Association('it_confindustria', 'Confindustria Moda', 'Italy', 'https://www.confindustriamoda.it'),
        Association('ch_swisstextiles', 'Swiss Textiles', 'Switzerland', 'https://www.swisstextiles.ch'),
        Association('tr_ttsis', 'TTSIS', 'Turkey', 'http://www.tekstilisveren.org.tr'),
        Association('tr_ihkib', 'IHKIB', 'Turkey', 'https://www.ihkib.org.tr'),
        Association('tr_ithib', 'ITHIB', 'Turkey', 'https://www.ithib.org.tr'),
        Association('uk_ukft', 'UKFT', 'United Kingdom', 'https://www.ukft.org'),
        Association('pt_atp', 'ATP Portugal', 'Portugal', 'https://atp.pt'),
        Association('es_cie', 'CIE Spain', 'Spain', 'https://consejointertextil.com'),
        Association('pl_piot', 'PIOT Poland', 'Poland', 'https://textiles.pl'),
        Association('fr_uit', 'UIT France', 'France', 'https://www.textile.fr'),
        Association('gr_hcia', 'HCIA Greece', 'Greece', 'https://www.hcia.eu'),
    ]
    
    # ITMF Members (from fetch_webpage results)
    ITMF_MEMBERS = [
        Association('ar_fita', 'FITA Argentina', 'Argentina', 'http://www.fita.com.ar'),
        Association('br_abit', 'ABIT Brazil', 'Brazil', 'http://www.abit.org.br'),
        Association('cn_cntac', 'CNTAC China', 'China', 'http://www.cntac.org.cn'),
        Association('eg_ecaht', 'ECAHT Egypt', 'Egypt', 'http://www.ecahtegypt.com'),
        Association('de_ivgt', 'IVGT Germany', 'Germany', 'http://www.ivgt.de'),
        Association('jp_jsa', 'JSA Japan', 'Japan', 'http://www.jsa-jp.org'),
        Association('kr_kofoti', 'KOFOTI Korea', 'Korea', 'http://www.kofoti.or.kr'),
        Association('ma_amith', 'AMITH Morocco', 'Morocco', 'http://www.amith.ma'),
        Association('tj_tajcottex', 'Tajcottex', 'Tajikistan', 'http://www.tajcottex.tj'),
        Association('uz_uztextileprom', 'Uztextileprom', 'Uzbekistan', 'http://www.uzts.uz'),
    ]
    
    def __init__(self, brave_api_key: Optional[str] = None):
        self.brave_api_key = brave_api_key
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.harvested: List[Member] = []
        self.seen_companies: Set[str] = set()
    
    def harvest_all(self) -> List[Dict]:
        """Harvest members from all known associations."""
        logger.info("Starting association members harvest...")
        
        # Combine all associations
        all_associations = self.EURATEX_MEMBERS + self.ITMF_MEMBERS
        
        for assoc in all_associations:
            try:
                members = self._harvest_association(assoc)
                logger.info(f"  {assoc.name}: {len(members)} members")
            except Exception as e:
                logger.warning(f"  {assoc.name}: Error - {e}")
            time.sleep(1)  # Rate limiting
        
        # Convert to lead format
        leads = self._to_leads()
        logger.info(f"Association harvest complete: {len(leads)} unique members")
        
        return leads
    
    def _harvest_association(self, assoc: Association) -> List[Member]:
        """Harvest members from a single association."""
        members = []
        
        # Try to find member directory
        directory_urls = [
            f"{assoc.url}/members",
            f"{assoc.url}/mitglieder",
            f"{assoc.url}/en/members",
            f"{assoc.url}/member-directory",
            f"{assoc.url}/our-members",
        ]
        
        for url in directory_urls:
            try:
                resp = self.session.get(url, timeout=10)
                if resp.status_code == 200:
                    members = self._parse_member_page(resp.text, assoc)
                    if members:
                        break
            except:
                continue
        
        # If no direct access, use Brave search
        if not members and self.brave_api_key:
            members = self._search_members_brave(assoc)
        
        # Add to harvested
        for member in members:
            if member.company.lower() not in self.seen_companies:
                self.seen_companies.add(member.company.lower())
                self.harvested.append(member)
        
        return members
    
    def _parse_member_page(self, html: str, assoc: Association) -> List[Member]:
        """Parse member directory HTML."""
        members = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for member listings in various formats
        # Format 1: Links with company domains
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Skip navigation links
            if len(text) < 3 or len(text) > 100:
                continue
            if any(skip in text.lower() for skip in ['learn more', 'read more', 'home', 'contact']):
                continue
            
            # Check if it looks like a company website
            if self._is_company_domain(href):
                members.append(Member(
                    company=text,
                    country=assoc.country,
                    website=href,
                    source_association=assoc.id,
                    evidence_url=assoc.url
                ))
        
        # Format 2: List items or divs with company info
        for item in soup.find_all(['li', 'div'], class_=re.compile(r'member|company|item', re.I)):
            text = item.get_text(strip=True)
            if 10 < len(text) < 100:
                # Extract company name (first line usually)
                lines = text.split('\n')
                company = lines[0].strip()
                
                # Find website in item
                website = None
                link = item.find('a', href=True)
                if link and self._is_company_domain(link['href']):
                    website = link['href']
                
                if company and not any(skip in company.lower() for skip in ['member', 'contact', 'more']):
                    members.append(Member(
                        company=company,
                        country=assoc.country,
                        website=website,
                        source_association=assoc.id,
                        evidence_url=assoc.url
                    ))
        
        return members
    
    def _search_members_brave(self, assoc: Association) -> List[Member]:
        """Search for association members using Brave API."""
        if not self.brave_api_key:
            return []
        
        members = []
        
        # Search queries
        queries = [
            f'site:{assoc.url} members',
            f'"{assoc.name}" member companies textile',
            f'{assoc.name} mitglieder unternehmen',
        ]
        
        for query in queries[:1]:  # Limit API calls
            try:
                resp = requests.get(
                    'https://api.search.brave.com/res/v1/web/search',
                    params={'q': query, 'count': 20},
                    headers={'X-Subscription-Token': self.brave_api_key},
                    timeout=10
                )
                
                if resp.status_code == 200:
                    data = resp.json()
                    for result in data.get('web', {}).get('results', []):
                        title = result.get('title', '')
                        url = result.get('url', '')
                        
                        # Try to extract company names from results
                        if assoc.country.lower() in title.lower() or 'textile' in title.lower():
                            members.append(Member(
                                company=title,
                                country=assoc.country,
                                website=url,
                                source_association=assoc.id,
                                evidence_url=url
                            ))
            except Exception as e:
                logger.debug(f"Brave search error: {e}")
        
        return members
    
    def _is_company_domain(self, url: str) -> bool:
        """Check if URL looks like a company website."""
        if not url:
            return False
        
        # Skip social media, etc.
        skip_domains = [
            'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
            'youtube.com', 'wikipedia.org', 'google.com', 'mailto:'
        ]
        
        url_lower = url.lower()
        return (
            ('http' in url_lower or 'www.' in url_lower) and
            not any(skip in url_lower for skip in skip_domains)
        )
    
    def _to_leads(self) -> List[Dict]:
        """Convert harvested members to lead format."""
        leads = []
        
        for member in self.harvested:
            lead = {
                'company': member.company,
                'country': member.country,
                'website': member.website or '',
                'source_type': 'association_member',
                'source': f'Association: {member.source_association}',
                'evidence_url': member.evidence_url,
                'city': member.city or '',
            }
            leads.append(lead)
        
        return leads
    
    def harvest_with_brave_search(self, target_countries: List[str] = None) -> List[Dict]:
        """
        Use Brave search to find textile finishing companies in target countries.
        """
        if not self.brave_api_key:
            logger.warning("No Brave API key - skipping web search harvest")
            return []
        
        if not target_countries:
            target_countries = ['Turkey', 'Egypt', 'Morocco', 'Pakistan', 'India', 'Brazil']
        
        leads = []
        
        queries = [
            '"{country}" textile finishing company dyeing',
            '"{country}" textile mill stenter machine',
            '"{country}" fabric dyehouse finishing',
            '"{country}" Brückner Monforts customer textile',
        ]
        
        for country in target_countries:
            for query_template in queries:
                query = query_template.format(country=country)
                try:
                    resp = requests.get(
                        'https://api.search.brave.com/res/v1/web/search',
                        params={'q': query, 'count': 20},
                        headers={'X-Subscription-Token': self.brave_api_key},
                        timeout=10
                    )
                    
                    if resp.status_code == 200:
                        data = resp.json()
                        for result in data.get('web', {}).get('results', []):
                            title = result.get('title', '')
                            url = result.get('url', '')
                            description = result.get('description', '')
                            
                            # Skip news/blog sites
                            if any(skip in url.lower() for skip in 
                                   ['news', 'blog', 'article', 'magazine', 'linkedin', 'facebook']):
                                continue
                            
                            leads.append({
                                'company': title,
                                'country': country,
                                'website': url,
                                'source_type': 'association_search',
                                'source': f'Brave: {query[:50]}',
                                'context': description,
                                'evidence_url': url,
                            })
                    
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    logger.debug(f"Search error: {e}")
        
        logger.info(f"Brave association search: {len(leads)} leads")
        return leads


def harvest_association_members(brave_api_key: Optional[str] = None) -> List[Dict]:
    """Convenience function to harvest association members."""
    harvester = AssociationMembersHarvester(brave_api_key)
    return harvester.harvest_all()


if __name__ == '__main__':
    import os
    
    logging.basicConfig(level=logging.INFO)
    
    # Try to get Brave API key from environment
    api_key = os.environ.get('BRAVE_API_KEY')
    
    harvester = AssociationMembersHarvester(api_key)
    leads = harvester.harvest_all()
    
    print(f"\n=== HARVESTED {len(leads)} MEMBERS ===")
    for lead in leads[:20]:
        print(f"  [{lead['country']}] {lead['company']} - {lead.get('website', 'No website')}")
