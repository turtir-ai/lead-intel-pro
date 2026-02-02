#!/usr/bin/env python3
"""
Colombiatex Exhibitor Harvester
GPT önerisi: Kolombiya tekstil fuarı exhibitor listesi

Colombiatex: Güney Amerika'nın en büyük tekstil fuarlarından biri
Inexmoda tarafından düzenleniyor
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.evidence import record_evidence
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


class ColombiatexHarvester:
    """
    Colombiatex fuar exhibitor listesini toplar.
    
    Kaynak: https://colombiatex.com
    Hedef: Tekstil üreticileri, finishing tesisleri
    """
    
    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.client = HttpClient(settings=settings, policies=policies)
        self.evidence_path = evidence_path
        
        # Colombiatex exhibitor URLs
        self.base_url = "https://colombiatex.com"
        self.exhibitor_urls = [
            "https://colombiatex.com/en/exhibitors/",
            "https://colombiatex.com/expositores/",
        ]
        
        # Finishing/dyeing keywords - öncelikli hedefler
        self.finishing_keywords = [
            'tinturería', 'tintorería', 'acabados', 'teñido', 'estampado',
            'blanqueo', 'termofijado', 'sanforizado', 'mercerizado',
            'finishing', 'dyeing', 'printing', 'bleaching', 'coating',
            'textil', 'tejido', 'confección', 'manufactura'
        ]
        
        # Machinery/supplier keywords - bunları ayır
        self.machinery_keywords = [
            'máquina', 'maquinaria', 'equipos', 'software', 'química',
            'importadora', 'distribuidora', 'comercializadora',
            'hilos', 'botones', 'cremalleras', 'etiquetas', 'accesorios'
        ]
    
    def harvest(self) -> List[Dict]:
        """Tüm Colombiatex exhibitor'larını topla."""
        all_leads = []
        
        logger.info("=" * 60)
        logger.info("🇨🇴 COLOMBIATEX EXHIBITOR HARVESTER")
        logger.info("=" * 60)
        
        for url in self.exhibitor_urls:
            try:
                leads = self._harvest_exhibitors(url)
                all_leads.extend(leads)
                logger.info(f"✅ {url}: {len(leads)} exhibitors")
            except Exception as e:
                logger.error(f"Error harvesting {url}: {e}")
        
        # Deduplicate by company name
        seen = set()
        unique_leads = []
        for lead in all_leads:
            key = lead.get("company", "").lower().strip()
            if key and key not in seen:
                seen.add(key)
                unique_leads.append(lead)
        
        logger.info(f"\n🇨🇴 COLOMBIATEX TOTAL: {len(unique_leads)} unique exhibitors")
        return unique_leads
    
    def _harvest_exhibitors(self, url: str) -> List[Dict]:
        """Parse exhibitor page."""
        leads = []
        
        html = self.client.get(url)
        if not html:
            logger.warning(f"Failed to fetch {url}")
            return leads
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Try different exhibitor card patterns
        exhibitor_cards = self._find_exhibitor_cards(soup)
        
        for card in exhibitor_cards:
            lead = self._parse_exhibitor_card(card, url)
            if lead:
                leads.append(lead)
        
        return leads
    
    def _find_exhibitor_cards(self, soup: BeautifulSoup) -> List:
        """Find exhibitor card elements with various selectors."""
        cards = []
        
        # Pattern 1: article.exhibitor
        cards.extend(soup.find_all('article', class_=lambda c: c and 'exhibitor' in str(c).lower()))
        
        # Pattern 2: div.exhibitor-card
        cards.extend(soup.find_all('div', class_=lambda c: c and 'exhibitor' in str(c).lower()))
        
        # Pattern 3: li.exhibitor-item
        cards.extend(soup.find_all('li', class_=lambda c: c and 'exhibitor' in str(c).lower()))
        
        # Pattern 4: table rows with company info
        for tr in soup.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) >= 2:
                text = tr.get_text(strip=True)
                if len(text) > 5:
                    cards.append(tr)
        
        # Pattern 5: accordion items
        cards.extend(soup.find_all('div', class_=lambda c: c and 'accordion' in str(c).lower()))
        
        # Pattern 6: Generic company name headers
        if not cards:
            cards = soup.find_all(['h3', 'h4', 'h5'], class_=lambda c: c and 'company' in str(c).lower() if c else False)
            if not cards:
                # Fallback: all h4 headers
                cards = soup.find_all('h4')
        
        return cards
    
    def _parse_exhibitor_card(self, card, source_url: str) -> Optional[Dict]:
        """Parse a single exhibitor card."""
        # Get company name
        name_elem = card.find(['h2', 'h3', 'h4', 'h5', 'strong', 'b'])
        if name_elem:
            company_name = name_elem.get_text(strip=True)
        else:
            company_name = card.get_text(strip=True)[:100]
        
        if not company_name or len(company_name) < 2:
            return None
        
        # Clean company name
        company_name = re.sub(r'\s+', ' ', company_name).strip()
        if len(company_name) > 100:
            company_name = company_name[:100]
        
        # Get full text for context
        full_text = card.get_text(separator=' ', strip=True)
        
        # Extract website
        website = ""
        links = card.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            if href.startswith('http') and 'colombiatex' not in href.lower():
                if not any(s in href.lower() for s in ['instagram', 'facebook', 'twitter', 'linkedin', 'youtube']):
                    website = href
                    break
        
        # Extract email
        email = ""
        email_link = card.find('a', href=lambda h: h and h.startswith('mailto:'))
        if email_link:
            email = email_link.get('href', '').replace('mailto:', '').split('?')[0]
        else:
            email_match = re.search(r'[\w.+-]+@[\w.-]+\.\w{2,}', full_text)
            if email_match:
                email = email_match.group()
        
        # Extract phone
        phone = ""
        phone_match = re.search(r'(?:Tel|Phone|Cel|Móvil)?[:\s]*(\+?\d[\d\s.-]{8,})', full_text)
        if phone_match:
            phone = phone_match.group(1).strip()
        
        # Determine company type
        text_lower = full_text.lower()
        is_finishing = any(kw in text_lower for kw in self.finishing_keywords)
        is_machinery = any(kw in text_lower for kw in self.machinery_keywords)
        
        # Record evidence
        content_hash = save_text_cache(f"{source_url}#{company_name}", full_text[:500])
        record_evidence(
            self.evidence_path,
            {
                "source_type": "fair",
                "source_name": "Colombiatex",
                "url": source_url,
                "title": company_name,
                "snippet": full_text[:400],
                "content_hash": content_hash,
                "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
            },
        )
        
        lead = {
            "company": company_name,
            "country": "Colombia",
            "source_type": "fair",
            "source": source_url,
            "source_name": "Colombiatex",
            "region": "south_america",
            "website": website,
            "emails": [email] if email else [],
            "phones": [phone] if phone else [],
            "context": full_text[:400],
            "has_finishing_context": is_finishing,
            "is_machinery_supplier": is_machinery,
            "priority": 1 if is_finishing and not is_machinery else (3 if is_machinery else 2),
            "harvested_at": datetime.utcnow().isoformat()
        }
        
        return lead


# Test
if __name__ == "__main__":
    harvester = ColombiatexHarvester()
    leads = harvester.harvest()
    
    print(f"\n{'='*60}")
    print(f"TOPLAM: {len(leads)} leads")
    
    # Website olan şirketler
    with_website = [l for l in leads if l.get('website')]
    print(f"Website olan: {len(with_website)}")
    
    # Finishing companies
    finishing = [l for l in leads if l.get('has_finishing_context')]
    print(f"Finishing context: {len(finishing)}")
    
    # Örnek leads
    print("\nÖrnek leads (ilk 10):")
    for lead in leads[:10]:
        print(f"  - {lead['company']} - {lead.get('website', 'no website')}")
