#!/usr/bin/env python3
"""
ITMF / EURATEX Association Bootstrap
GPT önerisi: Dünya çapında "birlik kapısı" - ülke ülke açılan gateway

ITMF: International Textile Manufacturers Federation
EURATEX: European Apparel and Textile Confederation

Bu collector:
1. ITMF/EURATEX üye listesinden ülke birliklerini çeker
2. Her ülke birliğinin sitesinden üye dizinini bulur
3. Gerçek tekstil fabrikalarını toplar
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.evidence import record_evidence
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


class ITMFBootstrap:
    """
    ITMF/EURATEX üzerinden global tekstil birliklerini keşfet ve üyelerini topla.
    
    Strateji:
    1. ITMF membership sayfasından ulusal birlikleri al
    2. Her birliğin sitesinde üye/socios/members dizini bul
    3. Üye şirketleri harvest et
    """
    
    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.client = HttpClient(settings=settings, policies=policies)
        self.evidence_path = evidence_path
        
        # ITMF membership page
        self.itmf_url = "https://www.itmf.org/membership/members"
        
        # EURATEX members page
        self.euratex_url = "https://euratex.eu/about-euratex/membership/our-members/"
        
        # Known national associations (manual seed)
        self.known_associations = {
            # South America
            "brazil": {
                "name": "ABIT - Associação Brasileira da Indústria Têxtil",
                "url": "https://www.abit.org.br/",
                "members_patterns": ["/associados", "/empresas", "/members", "/socios"]
            },
            "argentina": {
                "name": "FITA - Federación de Industrias Textiles Argentinas",
                "url": "https://www.fita.org.ar/",
                "members_patterns": ["/asociados", "/empresas", "/miembros"]
            },
            "colombia": {
                "name": "ANDI Textiles - Cámara Textil y Confecciones",
                "url": "https://www.andi.com.co/",
                "members_patterns": ["/empresas", "/miembros", "/afiliados"]
            },
            "peru": {
                "name": "SNI - Sociedad Nacional de Industrias",
                "url": "https://www.sni.org.pe/",
                "members_patterns": ["/asociados", "/directorio", "/empresas"]
            },
            "ecuador": {
                "name": "AITE - Asociación de Industriales Textiles del Ecuador",
                "url": "https://www.aite.com.ec/",
                "members_patterns": ["/socios", "/miembros", "/empresas"]
            },
            # Africa
            "egypt": {
                "name": "ETEC - Egypt Textile Exporters Council",
                "url": "https://www.textileegypt.org/",
                "members_patterns": ["/members", "/exporters", "/directory"]
            },
            "morocco": {
                "name": "AMITH - Association Marocaine des Industries du Textile",
                "url": "https://www.amith.ma/",
                "members_patterns": ["/membres", "/adherents", "/directory"]
            },
            "tunisia": {
                "name": "FTTH - Fédération Tunisienne du Textile et de l'Habillement",
                "url": "https://www.ftth.org.tn/",
                "members_patterns": ["/membres", "/adherents", "/annuaire"]
            },
            # Turkey
            "turkey": {
                "name": "ITKIB - Istanbul Textile and Apparel Exporter Associations",
                "url": "https://www.itkib.org.tr/",
                "members_patterns": ["/members", "/uyeler", "/firmalar"]
            },
            # India
            "india": {
                "name": "CITI - Confederation of Indian Textile Industry",
                "url": "https://www.citiindia.org/",
                "members_patterns": ["/members", "/directory", "/companies"]
            },
            # Pakistan
            "pakistan": {
                "name": "APTMA - All Pakistan Textile Mills Association",
                "url": "https://www.aptma.org.pk/",
                "members_patterns": ["/members", "/mills", "/directory"]
            },
            # Bangladesh
            "bangladesh": {
                "name": "BTMA - Bangladesh Textile Mills Association",
                "url": "https://www.btmadhaka.com/",
                "members_patterns": ["/members", "/mills", "/directory"]
            },
        }
        
        # Finishing keywords for prioritization
        self.finishing_keywords = [
            'dyeing', 'finishing', 'printing', 'bleaching', 'mercerizing',
            'terbiye', 'boyama', 'baskı', 'apre',  # Turkish
            'tinturaria', 'acabamento', 'estamparia',  # Portuguese
            'tintorería', 'acabados', 'estampado',  # Spanish
            'teinture', 'finissage', 'impression',  # French
        ]
    
    def harvest(self) -> List[Dict]:
        """Bootstrap all associations and collect members."""
        all_leads = []
        discovered_associations = []
        
        logger.info("=" * 60)
        logger.info("🌍 ITMF/EURATEX ASSOCIATION BOOTSTRAP")
        logger.info("=" * 60)
        
        # Step 1: Discover associations from ITMF
        logger.info("\n📋 Step 1: Discovering associations from ITMF...")
        itmf_associations = self._discover_itmf_associations()
        discovered_associations.extend(itmf_associations)
        logger.info(f"  Found {len(itmf_associations)} associations from ITMF")
        
        # Step 2: Discover from EURATEX
        logger.info("\n📋 Step 2: Discovering associations from EURATEX...")
        euratex_associations = self._discover_euratex_associations()
        discovered_associations.extend(euratex_associations)
        logger.info(f"  Found {len(euratex_associations)} associations from EURATEX")
        
        # Step 3: Add known associations
        logger.info("\n📋 Step 3: Adding known associations...")
        for country, assoc in self.known_associations.items():
            discovered_associations.append({
                "name": assoc["name"],
                "url": assoc["url"],
                "country": country.title(),
                "members_patterns": assoc["members_patterns"]
            })
        logger.info(f"  Added {len(self.known_associations)} known associations")
        
        # Deduplicate associations by domain
        unique_associations = self._dedupe_associations(discovered_associations)
        logger.info(f"\n📊 Total unique associations: {len(unique_associations)}")
        
        # Step 4: Harvest members from each association
        logger.info("\n📋 Step 4: Harvesting members from associations...")
        for assoc in unique_associations:
            try:
                members = self._harvest_association_members(assoc)
                all_leads.extend(members)
                logger.info(f"  ✅ {assoc['name']}: {len(members)} members")
            except Exception as e:
                logger.warning(f"  ❌ {assoc['name']}: {e}")
        
        # Deduplicate leads
        seen = set()
        unique_leads = []
        for lead in all_leads:
            key = lead.get("company", "").lower().strip()
            if key and key not in seen:
                seen.add(key)
                unique_leads.append(lead)
        
        logger.info(f"\n🌍 ITMF BOOTSTRAP TOTAL: {len(unique_leads)} unique leads")
        return unique_leads
    
    def _discover_itmf_associations(self) -> List[Dict]:
        """Discover national associations from ITMF membership page."""
        associations = []
        
        html = self.client.get(self.itmf_url)
        if not html:
            logger.warning("Failed to fetch ITMF page")
            return associations
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find member organization cards/links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Skip ITMF internal links
            if 'itmf.org' in href:
                continue
            
            if href.startswith('http') and len(text) > 5:
                # Check if it looks like an association
                if any(kw in text.lower() for kw in ['association', 'federation', 'council', 'union', 'chamber']):
                    associations.append({
                        "name": text,
                        "url": href,
                        "country": self._guess_country(text, href),
                        "members_patterns": ["/members", "/directory", "/companies"]
                    })
        
        return associations
    
    def _discover_euratex_associations(self) -> List[Dict]:
        """Discover European associations from EURATEX."""
        associations = []
        
        html = self.client.get(self.euratex_url)
        if not html:
            logger.warning("Failed to fetch EURATEX page")
            return associations
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find member links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            if 'euratex' in href:
                continue
            
            if href.startswith('http') and len(text) > 5:
                associations.append({
                    "name": text,
                    "url": href,
                    "country": self._guess_country(text, href),
                    "members_patterns": ["/members", "/membres", "/mitglieder", "/unternehmen"]
                })
        
        return associations
    
    def _dedupe_associations(self, associations: List[Dict]) -> List[Dict]:
        """Deduplicate associations by domain."""
        seen_domains = set()
        unique = []
        
        for assoc in associations:
            try:
                domain = urlparse(assoc["url"]).netloc.lower()
                domain = domain.replace("www.", "")
                if domain and domain not in seen_domains:
                    seen_domains.add(domain)
                    unique.append(assoc)
            except:
                continue
        
        return unique
    
    def _guess_country(self, name: str, url: str) -> str:
        """Guess country from association name or URL."""
        name_lower = name.lower()
        url_lower = url.lower()
        
        country_patterns = {
            "brazil": ["brazil", "brasil", ".br"],
            "argentina": ["argentina", ".ar"],
            "colombia": ["colombia", ".co"],
            "peru": ["peru", ".pe"],
            "ecuador": ["ecuador", ".ec"],
            "egypt": ["egypt", ".eg"],
            "morocco": ["morocco", "maroc", ".ma"],
            "tunisia": ["tunisia", "tunisie", ".tn"],
            "turkey": ["turkey", "türk", ".tr"],
            "india": ["india", ".in"],
            "pakistan": ["pakistan", ".pk"],
            "bangladesh": ["bangladesh", ".bd"],
            "germany": ["germany", "deutsch", ".de"],
            "italy": ["italy", "italia", ".it"],
            "france": ["france", ".fr"],
            "spain": ["spain", "españa", ".es"],
            "portugal": ["portugal", ".pt"],
        }
        
        for country, patterns in country_patterns.items():
            for pattern in patterns:
                if pattern in name_lower or pattern in url_lower:
                    return country.title()
        
        return "Unknown"
    
    def _harvest_association_members(self, association: Dict) -> List[Dict]:
        """Harvest members from an association website."""
        leads = []
        base_url = association["url"]
        
        # Try to find member directory
        for pattern in association.get("members_patterns", []):
            member_url = urljoin(base_url, pattern)
            
            html = self.client.get(member_url)
            if not html:
                continue
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find company elements
            for card in self._find_company_elements(soup):
                lead = self._parse_member(card, member_url, association)
                if lead:
                    leads.append(lead)
            
            if leads:
                break  # Found members, stop trying other patterns
        
        return leads
    
    def _find_company_elements(self, soup: BeautifulSoup) -> List:
        """Find company card elements."""
        cards = []
        
        patterns = [
            ('article', 'member'),
            ('div', 'member'),
            ('div', 'company'),
            ('div', 'empresa'),
            ('div', 'socio'),
            ('li', 'member'),
            ('tr', None),
        ]
        
        for tag, class_pattern in patterns:
            if class_pattern:
                found = soup.find_all(tag, class_=lambda c: c and class_pattern in str(c).lower())
            else:
                found = soup.find_all(tag)
            cards.extend(found)
        
        if not cards:
            cards = soup.find_all(['h3', 'h4', 'h5'])
        
        return cards[:100]  # Limit to first 100 to avoid huge lists
    
    def _parse_member(self, card, source_url: str, association: Dict) -> Optional[Dict]:
        """Parse member info from card."""
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
            if href.startswith('http'):
                parsed = urlparse(href)
                # Skip social and association domains
                if not any(s in parsed.netloc.lower() for s in ['instagram', 'facebook', 'twitter', 'linkedin', 'youtube', 'wa.me']):
                    if association["url"] not in href:
                        website = href
                        break
        
        # Email
        email = ""
        email_match = re.search(r'[\w.+-]+@[\w.-]+\.\w{2,}', full_text)
        if email_match:
            email = email_match.group()
        
        # Phone
        phone = ""
        phone_match = re.search(r'(\+?\d[\d\s.-]{8,})', full_text)
        if phone_match:
            phone = phone_match.group(1).strip()
        
        # Finishing company check
        text_lower = full_text.lower()
        is_finishing = any(kw in text_lower for kw in self.finishing_keywords)
        
        # Evidence
        content_hash = save_text_cache(f"{source_url}#{company_name}", full_text[:500])
        record_evidence(
            self.evidence_path,
            {
                "source_type": "association",
                "source_name": association["name"],
                "url": source_url,
                "title": company_name,
                "snippet": full_text[:400],
                "content_hash": content_hash,
                "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
            },
        )
        
        lead = {
            "company": company_name,
            "country": association.get("country", "Unknown"),
            "source_type": "association",
            "source": source_url,
            "source_name": association["name"],
            "website": website,
            "emails": [email] if email else [],
            "phones": [phone] if phone else [],
            "context": full_text[:400],
            "has_finishing_context": is_finishing,
            "priority": 1 if is_finishing else 2,
            "harvested_at": datetime.utcnow().isoformat()
        }
        
        return lead


# Test
if __name__ == "__main__":
    bootstrap = ITMFBootstrap()
    leads = bootstrap.harvest()
    
    print(f"\n{'='*60}")
    print(f"TOPLAM: {len(leads)} leads")
    
    # Country distribution
    by_country = {}
    for lead in leads:
        country = lead.get('country', 'Unknown')
        by_country[country] = by_country.get(country, 0) + 1
    
    print("\nÜlke dağılımı:")
    for country, count in sorted(by_country.items(), key=lambda x: -x[1]):
        print(f"  {country}: {count}")
    
    with_website = [l for l in leads if l.get('website')]
    print(f"\nWebsite olan: {len(with_website)}")
    
    finishing = [l for l in leads if l.get('has_finishing_context')]
    print(f"Finishing context: {len(finishing)}")
