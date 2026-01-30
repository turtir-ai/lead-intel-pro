#!/usr/bin/env python3
"""
South America Directory Collector - SA için özel kaynak toplayıcı
ChatGPT Audit önerisi: Facility/mill dizinleri ve fuar exhibitor listeleri

Kaynaklar:
- AITE Ecuador (socios.html) - Tekstil birliği üyeleri
- Febratex Brazil - Fuar exhibitor listesi  
- Colombiatex/Inexmoda - Colombia exhibitors
- CIAI Argentina - Camara Industrial Argentina
"""

import os
import re
import logging
from typing import Dict, List, Optional
from urllib.parse import urljoin
from datetime import datetime

from bs4 import BeautifulSoup

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SouthAmericaDirectoryCollector:
    """
    Güney Amerika tekstil dizin ve fuar kaynakları toplayıcısı.
    
    Hedef: Gerçek tesis/mill bilgileri + website + telefon
    """
    
    def __init__(self):
        self.client = HttpClient()
        
        # SA kaynakları
        self.sources = {
            "aite_ecuador": {
                "url": "https://www.aite.com.ec/socios.html",
                "name": "AITE Ecuador - Socios",
                "country": "Ecuador",
                "type": "association_member"
            },
            "febratex_brazil": {
                "url": "https://febratex.com.br/inscricoes/",
                "name": "FEBRATEX 2026 Exhibitors", 
                "country": "Brazil",
                "type": "fair"
            },
            # Colombiatex için ayrı URL gerekecek (dinamik site)
        }
        
        # Finishing/dyeing keywords - bunları içeren şirketler öncelikli
        self.finishing_keywords = [
            'tinturación', 'tinturaria', 'acabamento', 'acabado',
            'teñido', 'tingimento', 'estampado', 'estamparia',
            'blanqueo', 'alvejamento', 'termofijado', 'termofixação',
            'perchado', 'chamuscado', 'sanforizado', 'mercerizacao',
            'terbiye', 'finishing', 'dyeing', 'printing', 'bleaching'
        ]
        
        # Machinery/equipment keywords - bunlar makine tedarikçisi olabilir
        self.machinery_keywords = [
            'máquina', 'maquina', 'máquinas', 'equipamento', 'equipment',
            'importadora', 'distribuidora', 'software', 'química', 'quimica'
        ]
    
    def harvest(self) -> List[Dict]:
        """Tüm SA kaynaklarından lead topla."""
        all_leads = []
        
        logger.info("=" * 60)
        logger.info("🌎 SOUTH AMERICA DIRECTORY COLLECTOR")
        logger.info("=" * 60)
        
        # 1. AITE Ecuador
        aite_leads = self._collect_aite_ecuador()
        all_leads.extend(aite_leads)
        logger.info(f"✅ AITE Ecuador: {len(aite_leads)} leads")
        
        # 2. Febratex Brazil
        febratex_leads = self._collect_febratex()
        all_leads.extend(febratex_leads)
        logger.info(f"✅ Febratex Brazil: {len(febratex_leads)} leads")
        
        logger.info(f"\n🌎 TOTAL SA DIRECTORY LEADS: {len(all_leads)}")
        return all_leads
    
    def _collect_aite_ecuador(self) -> List[Dict]:
        """
        AITE (Asociación de Industriales Textiles del Ecuador) üyelerini topla.
        
        Sayfa formatı:
        - Company name: <h6> içinde
        - City: İlk satır
        - Address: DIRECCIÓN: ...
        - Phone: TELÉFONO: ...
        - Website: PÁGINA WEB: <a href="...">
        - Products: Son satır
        """
        leads = []
        url = self.sources["aite_ecuador"]["url"]
        
        try:
            html = self.client.get(url)
            if not html:
                logger.warning("AITE page fetch failed")
                return leads
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Her şirket bir kart içinde
            # Pattern: h6 > company name, sonra p'ler içinde bilgi
            company_headers = soup.find_all('h6')
            
            for header in company_headers:
                company_name = header.get_text(strip=True)
                if not company_name or len(company_name) < 3:
                    continue
                
                # Sonraki elementlerden bilgi çek
                parent = header.find_parent()
                if not parent:
                    continue
                
                text_content = parent.get_text(separator='\n', strip=True)
                
                # Website bul
                website = ""
                website_link = parent.find('a', href=True)
                if website_link:
                    href = website_link.get('href', '')
                    if href and 'http' in href:
                        website = href
                
                # Telefon bul
                phone = ""
                phone_match = re.search(r'TELÉFONO:\s*([^\n]+)', text_content)
                if phone_match:
                    phone = phone_match.group(1).strip()
                
                # Ürün bilgisi bul (son satır genelde ürün)
                lines = text_content.split('\n')
                products = ""
                for line in reversed(lines):
                    line = line.strip()
                    if line and not any(k in line.upper() for k in ['DIRECCIÓN', 'TELÉFONO', 'FAX', 'PÁGINA']):
                        if len(line) > 10:
                            products = line
                            break
                
                # Finishing company mi kontrol et
                is_finishing = any(kw in products.lower() or kw in company_name.lower() 
                                  for kw in self.finishing_keywords)
                
                lead = {
                    "company": company_name,
                    "country": "Ecuador",
                    "source_type": "directory",
                    "source": url,
                    "source_name": "AITE Ecuador",
                    "region": "south_america",
                    "website": website,
                    "phones": [phone] if phone else [],
                    "context": products,
                    "has_finishing_context": is_finishing,
                    "priority": 1 if is_finishing else 2,
                    "harvested_at": datetime.utcnow().isoformat()
                }
                
                leads.append(lead)
                logger.debug(f"  + {company_name} ({website or 'no website'})")
            
        except Exception as e:
            logger.error(f"AITE collection error: {e}")
        
        return leads
    
    def _collect_febratex(self) -> List[Dict]:
        """
        Febratex (Feira Brasileira para a Indústria Têxtil) exhibitor listesini topla.
        
        Sayfa formatı:
        - Company name: <h4> içinde
        - Segment: **Segmento: ...** (Máquina, Fio, Software, etc.)
        - Website: Site: <a href="...">
        """
        leads = []
        url = self.sources["febratex_brazil"]["url"]
        
        try:
            html = self.client.get(url)
            if not html:
                logger.warning("Febratex page fetch failed")
                return leads
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # h4 içinde şirket isimleri
            company_headers = soup.find_all('h4')
            
            for header in company_headers:
                company_name = header.get_text(strip=True)
                if not company_name or len(company_name) < 2:
                    continue
                
                # Sonraki sibling'lerden bilgi çek
                next_elem = header.find_next_sibling()
                segment = ""
                website = ""
                
                # Tüm sibling'leri kontrol et (bir sonraki h4'e kadar)
                current = header.next_sibling
                while current:
                    if hasattr(current, 'name') and current.name == 'h4':
                        break
                    
                    if hasattr(current, 'get_text'):
                        text = current.get_text(strip=True)
                        
                        # Segment
                        if 'Segmento:' in text:
                            segment_match = re.search(r'Segmento:\s*(\w+)', text)
                            if segment_match:
                                segment = segment_match.group(1)
                        
                        # Website
                        if 'Site:' in text:
                            link = current.find('a', href=True)
                            if link:
                                website = link.get('href', '')
                    
                    current = current.next_sibling
                
                # Machinery/equipment şirketlerini işaretle (makine tedarikçisi olabilir)
                is_machinery = segment.lower() in ['máquina', 'maquina', 'software', 
                                                    'automação', 'equipamento', 'química']
                
                # Textile manufacturer mı?
                is_textile = segment.lower() in ['fio', 'fibra', 'tecido', 'malha', 
                                                  'acabamento', 'tinturaria']
                
                lead = {
                    "company": company_name,
                    "country": "Brazil",
                    "source_type": "fair",
                    "source": url,
                    "source_name": "FEBRATEX 2026",
                    "region": "south_america",
                    "website": website,
                    "context": f"Feira FEBRATEX - Segmento: {segment}" if segment else "Feira FEBRATEX",
                    "segment": segment,
                    "is_machinery_supplier": is_machinery,
                    "is_textile_company": is_textile,
                    "priority": 1 if is_textile else (3 if is_machinery else 2),
                    "harvested_at": datetime.utcnow().isoformat()
                }
                
                leads.append(lead)
            
        except Exception as e:
            logger.error(f"Febratex collection error: {e}")
        
        return leads
    
    def _is_finishing_company(self, context: str) -> bool:
        """Check if company context suggests finishing/dyeing operations."""
        context_lower = context.lower()
        return any(kw in context_lower for kw in self.finishing_keywords)


# Test
if __name__ == "__main__":
    collector = SouthAmericaDirectoryCollector()
    leads = collector.harvest()
    
    print(f"\n{'='*60}")
    print(f"TOPLAM: {len(leads)} leads")
    
    # Ülke dağılımı
    by_country = {}
    for lead in leads:
        country = lead.get('country', 'Unknown')
        by_country[country] = by_country.get(country, 0) + 1
    
    print("\nÜlke dağılımı:")
    for country, count in sorted(by_country.items(), key=lambda x: -x[1]):
        print(f"  {country}: {count}")
    
    # Website olan şirketler
    with_website = [l for l in leads if l.get('website')]
    print(f"\nWebsite olan: {len(with_website)}")
    
    # Örnek leads
    print("\nÖrnek leads (ilk 10):")
    for lead in leads[:10]:
        print(f"  - {lead['company']} ({lead['country']}) - {lead.get('website', 'no website')}")
