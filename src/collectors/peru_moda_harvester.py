#!/usr/bin/env python3
"""
Peru Moda / ADEX Directory Harvester
GPT önerisi: Peru tekstil fuarı ve dernek üye listesi

Peru Moda: Peru'nun ana tekstil ve moda fuarı
ADEX: Asociación de Exportadores - Peru ihracatçılar derneği
SNI: Sociedad Nacional de Industrias - Peru sanayi derneği
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


class PeruModaHarvester:
    """
    Peru Moda fuar + ADEX/SNI üye listelerini toplar.
    
    Kaynaklar:
    - Peru Moda exhibitors
    - ADEX directory
    - SNI (Sociedad Nacional de Industrias) üyeleri
    
    Hedef: Peru tekstil üreticileri ve finishing tesisleri
    """
    
    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.client = HttpClient(settings=settings, policies=policies)
        self.evidence_path = evidence_path
        
        # Peru sources
        self.sources = {
            "peru_moda": {
                "urls": [
                    "https://perumoda.com/en/exhibitors/",
                    "https://perumoda.com/expositores/",
                ],
                "name": "Peru Moda",
                "type": "fair"
            },
            "adex": {
                "urls": [
                    "https://www.adexperu.org.pe/asociados/",
                    "https://www.adexperu.org.pe/directorio/",
                ],
                "name": "ADEX Peru",
                "type": "association"
            },
            "sni": {
                "urls": [
                    "https://www.sni.org.pe/asociados/",
                    "https://www.sni.org.pe/directorio/",
                ],
                "name": "SNI Peru",
                "type": "association"
            }
        }
        
        # Finishing/textile keywords
        self.finishing_keywords = [
            'tintorería', 'teñido', 'acabados', 'estampado', 'blanqueo',
            'textil', 'tejido', 'confección', 'hilado', 'tejidos',
            'algodón', 'alpaca', 'vicuña', 'pima',  # Peru-specific fibers
            'finishing', 'dyeing', 'printing', 'textile'
        ]
        
        # Machinery keywords
        self.machinery_keywords = [
            'máquina', 'maquinaria', 'equipos', 'software',
            'importadora', 'distribuidora', 'comercializadora',
            'accesorios', 'insumos', 'químicos'
        ]
    
    def harvest(self) -> List[Dict]:
        """Tüm Peru kaynaklarından lead topla."""
        all_leads = []
        
        logger.info("=" * 60)
        logger.info("🇵🇪 PERU MODA / ADEX / SNI HARVESTER")
        logger.info("=" * 60)
        
        for source_key, source_config in self.sources.items():
            for url in source_config["urls"]:
                try:
                    leads = self._harvest_page(
                        url, 
                        source_name=source_config["name"],
                        source_type=source_config["type"]
                    )
                    all_leads.extend(leads)
                    logger.info(f"✅ {source_config['name']}: {len(leads)} leads from {url}")
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
        
        logger.info(f"\n🇵🇪 PERU TOTAL: {len(unique_leads)} unique leads")
        return unique_leads
    
    def _harvest_page(self, url: str, source_name: str, source_type: str) -> List[Dict]:
        """Parse page for companies."""
        leads = []
        
        html = self.client.get(url)
        if not html:
            logger.warning(f"Failed to fetch {url}")
            return leads
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find company elements
        cards = self._find_company_elements(soup)
        
        for card in cards:
            lead = self._parse_company(card, url, source_name, source_type)
            if lead:
                leads.append(lead)
        
        return leads
    
    def _find_company_elements(self, soup: BeautifulSoup) -> List:
        """Find company card elements."""
        cards = []
        
        # Common patterns for exhibitor/member lists
        patterns = [
            ('article', 'exhibitor'),
            ('div', 'exhibitor'),
            ('div', 'empresa'),
            ('div', 'company'),
            ('div', 'asociado'),
            ('div', 'member'),
            ('li', 'exhibitor'),
            ('div', 'card'),
            ('div', 'item'),
        ]
        
        for tag, class_pattern in patterns:
            found = soup.find_all(tag, class_=lambda c: c and class_pattern in str(c).lower())
            cards.extend(found)
        
        # Table rows
        for table in soup.find_all('table'):
            for tr in table.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) >= 1:
                    text = tr.get_text(strip=True)
                    if len(text) > 5:
                        cards.append(tr)
        
        # Fallback: headers
        if not cards:
            cards = soup.find_all(['h3', 'h4', 'h5'])
        
        return cards
    
    def _parse_company(self, card, source_url: str, source_name: str, source_type: str) -> Optional[Dict]:
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
            if href.startswith('http') and 'peru' not in href.lower() and 'adex' not in href.lower():
                if not any(s in href.lower() for s in ['instagram', 'facebook', 'twitter', 'linkedin', 'youtube', 'wa.me']):
                    website = href
                    break
        
        # Email
        email = ""
        email_link = card.find('a', href=lambda h: h and h.startswith('mailto:'))
        if email_link:
            email = email_link.get('href', '').replace('mailto:', '').split('?')[0]
        else:
            email_match = re.search(r'[\w.+-]+@[\w.-]+\.\w{2,}', full_text)
            if email_match:
                email = email_match.group()
        
        # Phone
        phone = ""
        phone_match = re.search(r'(?:Tel|Teléfono|Cel)?[:\s]*(\+?\d[\d\s.-]{8,})', full_text)
        if phone_match:
            phone = phone_match.group(1).strip()
        
        # Company type
        text_lower = full_text.lower()
        is_finishing = any(kw in text_lower for kw in self.finishing_keywords)
        is_machinery = any(kw in text_lower for kw in self.machinery_keywords)
        
        # Peru-specific: Pima cotton and alpaca are high-quality fibers
        is_premium_fiber = any(kw in text_lower for kw in ['pima', 'alpaca', 'vicuña'])
        
        # Evidence
        content_hash = save_text_cache(f"{source_url}#{company_name}", full_text[:500])
        record_evidence(
            self.evidence_path,
            {
                "source_type": source_type,
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
            "country": "Peru",
            "source_type": source_type,
            "source": source_url,
            "source_name": source_name,
            "region": "south_america",
            "website": website,
            "emails": [email] if email else [],
            "phones": [phone] if phone else [],
            "context": full_text[:400],
            "has_finishing_context": is_finishing,
            "is_premium_fiber": is_premium_fiber,
            "is_machinery_supplier": is_machinery,
            "priority": 1 if (is_finishing or is_premium_fiber) and not is_machinery else (3 if is_machinery else 2),
            "harvested_at": datetime.utcnow().isoformat()
        }
        
        return lead


# Test
if __name__ == "__main__":
    harvester = PeruModaHarvester()
    leads = harvester.harvest()
    
    print(f"\n{'='*60}")
    print(f"TOPLAM: {len(leads)} leads")
    
    with_website = [l for l in leads if l.get('website')]
    print(f"Website olan: {len(with_website)}")
    
    finishing = [l for l in leads if l.get('has_finishing_context')]
    print(f"Finishing context: {len(finishing)}")
    
    premium = [l for l in leads if l.get('is_premium_fiber')]
    print(f"Premium fiber (Pima/Alpaca): {len(premium)}")
    
    print("\nÖrnek leads (ilk 10):")
    for lead in leads[:10]:
        print(f"  - {lead['company']} - {lead.get('website', 'no website')}")
