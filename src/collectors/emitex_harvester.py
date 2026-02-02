#!/usr/bin/env python3
"""
Emitex Simatex Confemaq Exhibitor Harvester
GPT önerisi: Arjantin tekstil fuarı exhibitor listesi

Emitex: Arjantin'in ana tekstil makineleri ve ekipmanları fuarı
Simatex ve Confemaq ile birlikte düzenleniyor
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


class EmitexHarvester:
    """
    Emitex/Simatex/Confemaq fuar exhibitor listesini toplar.
    
    Kaynak: https://emitex.ar / simatex.ar
    Hedef: Arjantin ve bölge tekstil üreticileri
    """
    
    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.client = HttpClient(settings=settings, policies=policies)
        self.evidence_path = evidence_path
        
        # Emitex exhibitor URLs
        self.exhibitor_urls = [
            "https://emitex.ar/expositores/",
            "https://emitex.ar/en/exhibitors/",
            "https://www.simatex.ar/expositores/",
            "https://confemaq.ar/expositores/",
        ]
        
        # Argentine textile association
        self.association_urls = [
            "https://www.fundacionprotejer.org/asociados/",  # ProTejer
            "https://www.ciaindumentaria.com.ar/empresas/",   # CIAI
        ]
        
        # Finishing/textile keywords
        self.finishing_keywords = [
            'tintorería', 'teñido', 'acabados', 'estampado', 'blanqueo',
            'termofijado', 'sanforizado', 'mercerizado', 'planchado',
            'textil', 'tejido', 'tejeduría', 'confección', 'hilandería',
            'finishing', 'dyeing', 'printing', 'bleaching'
        ]
        
        # Machinery keywords (to identify suppliers)
        self.machinery_keywords = [
            'máquina', 'maquinaria', 'equipos', 'repuestos', 'partes',
            'importadora', 'distribuidora', 'representante',
            'software', 'química', 'colorantes', 'insumos'
        ]
    
    def harvest(self) -> List[Dict]:
        """Tüm Emitex/Simatex/Confemaq + association kaynaklarından exhibitor topla."""
        all_leads = []
        
        logger.info("=" * 60)
        logger.info("🇦🇷 EMITEX/SIMATEX/CONFEMAQ EXHIBITOR HARVESTER")
        logger.info("=" * 60)
        
        # Fair exhibitors
        for url in self.exhibitor_urls:
            try:
                leads = self._harvest_page(url, source_name="Emitex Argentina")
                all_leads.extend(leads)
                logger.info(f"✅ {url}: {len(leads)} exhibitors")
            except Exception as e:
                logger.warning(f"Error harvesting {url}: {e}")
        
        # Association members
        for url in self.association_urls:
            try:
                source_name = "ProTejer" if "protejer" in url else "CIAI Argentina"
                leads = self._harvest_page(url, source_name=source_name)
                all_leads.extend(leads)
                logger.info(f"✅ {url}: {len(leads)} members")
            except Exception as e:
                logger.warning(f"Error harvesting {url}: {e}")
        
        # Deduplicate
        seen = set()
        unique_leads = []
        for lead in all_leads:
            key = lead.get("company", "").lower().strip()
            if key and key not in seen:
                seen.add(key)
                unique_leads.append(lead)
        
        logger.info(f"\n🇦🇷 ARGENTINA TOTAL: {len(unique_leads)} unique leads")
        return unique_leads
    
    def _harvest_page(self, url: str, source_name: str) -> List[Dict]:
        """Parse exhibitor/member page."""
        leads = []
        
        html = self.client.get(url)
        if not html:
            logger.warning(f"Failed to fetch {url}")
            return leads
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find exhibitor elements
        cards = self._find_company_elements(soup)
        
        for card in cards:
            lead = self._parse_company(card, url, source_name)
            if lead:
                leads.append(lead)
        
        return leads
    
    def _find_company_elements(self, soup: BeautifulSoup) -> List:
        """Find company card elements."""
        cards = []
        
        # Common patterns
        patterns = [
            ('article', 'exhibitor'),
            ('div', 'exhibitor'),
            ('div', 'empresa'),
            ('div', 'company'),
            ('li', 'exhibitor'),
            ('div', 'card'),
        ]
        
        for tag, class_pattern in patterns:
            found = soup.find_all(tag, class_=lambda c: c and class_pattern in str(c).lower())
            cards.extend(found)
        
        # Table rows
        for tr in soup.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) >= 2:
                cards.append(tr)
        
        # Fallback: headers
        if not cards:
            cards = soup.find_all(['h3', 'h4', 'h5'])
        
        return cards
    
    def _parse_company(self, card, source_url: str, source_name: str) -> Optional[Dict]:
        """Parse company info from card."""
        # Get company name
        name_elem = card.find(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'a'])
        if name_elem:
            company_name = name_elem.get_text(strip=True)
        else:
            company_name = card.get_text(strip=True)[:100]
        
        if not company_name or len(company_name) < 2:
            return None
        
        company_name = re.sub(r'\s+', ' ', company_name).strip()
        if len(company_name) > 100:
            company_name = company_name[:100]
        
        full_text = card.get_text(separator=' ', strip=True)
        
        # Website
        website = ""
        for link in card.find_all('a', href=True):
            href = link.get('href', '')
            if href.startswith('http') and 'emitex' not in href.lower() and 'simatex' not in href.lower():
                if not any(s in href.lower() for s in ['instagram', 'facebook', 'twitter', 'linkedin', 'youtube', 'wa.me']):
                    website = href
                    break
        
        # Email
        email = ""
        email_match = re.search(r'[\w.+-]+@[\w.-]+\.\w{2,}', full_text)
        if email_match:
            email = email_match.group()
        
        # Phone
        phone = ""
        phone_match = re.search(r'(?:Tel|Teléfono)?[:\s]*(\+?\d[\d\s.-]{8,})', full_text)
        if phone_match:
            phone = phone_match.group(1).strip()
        
        # Company type
        text_lower = full_text.lower()
        is_finishing = any(kw in text_lower for kw in self.finishing_keywords)
        is_machinery = any(kw in text_lower for kw in self.machinery_keywords)
        
        # Evidence
        content_hash = save_text_cache(f"{source_url}#{company_name}", full_text[:500])
        record_evidence(
            self.evidence_path,
            {
                "source_type": "fair" if "expositores" in source_url else "association",
                "source_name": source_name,
                "url": source_url,
                "title": company_name,
                "snippet": full_text[:400],
                "content_hash": content_hash,
                "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
            },
        )
        
        lead = {
            "company": company_name,
            "country": "Argentina",
            "source_type": "fair" if "expositores" in source_url else "association",
            "source": source_url,
            "source_name": source_name,
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
    harvester = EmitexHarvester()
    leads = harvester.harvest()
    
    print(f"\n{'='*60}")
    print(f"TOPLAM: {len(leads)} leads")
    
    with_website = [l for l in leads if l.get('website')]
    print(f"Website olan: {len(with_website)}")
    
    finishing = [l for l in leads if l.get('has_finishing_context')]
    print(f"Finishing context: {len(finishing)}")
    
    print("\nÖrnek leads (ilk 10):")
    for lead in leads[:10]:
        print(f"  - {lead['company']} - {lead.get('website', 'no website')}")
