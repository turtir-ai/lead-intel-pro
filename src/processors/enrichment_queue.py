#!/usr/bin/env python3
"""
Enrichment Queue - Website bulunamayan leads için Brave Search ile zenginleştirme
GPT önerisi: "website_domain non-company ise Brave ile resmi site bul"

Lead'de website yoksa veya yanlış domain varsa:
1. Brave Search ile şirket adı + ülke + tekstil ara
2. En uygun website'ı seç
3. Contact sayfasından email/phone çıkar
"""

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from src.processors.website_discovery import WebsiteDiscovery
from src.processors.contact_enricher import ContactEnricher
from src.utils.http_client import HttpClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


# Non-company domains that indicate lead needs enrichment
NON_COMPANY_DOMAINS = {
    # Certification/registry sites
    'global-standard.org', 'global-trace-base.org', 'oeko-tex.com', 
    'bluesign.com', 'bettercotton.org', 'textileexchange.org',
    # Association sites
    'abit.org.br', 'texbrasil.com.br', 'amith.ma', 'aite.com.ec',
    'itkib.org.tr', 'aptma.org.pk', 'btmadhaka.com',
    # Fair sites
    'febratex.com.br', 'colombiatex.com', 'perumoda.com', 'emitex.ar',
    'heimtextil.messefrankfurt.com', 'texworld.messefrankfurt.com',
    # Social media
    'instagram.com', 'facebook.com', 'twitter.com', 'linkedin.com', 
    'youtube.com', 'tiktok.com', 'pinterest.com', 'wa.me',
    # Marketplaces
    'alibaba.com', 'aliexpress.com', 'made-in-china.com', 'indiamart.com',
    'thomasnet.com', 'europages.com', 'kompass.com',
}


class EnrichmentQueue:
    """
    Website eksik veya yanlış olan leads için enrichment kuyruğu.
    
    Strateji:
    1. Leads'i tara, website kontrolü yap
    2. Eksik/yanlış olanları kuyruğa al
    3. Brave Search ile doğru website bul
    4. Contact sayfasından email/phone çıkar
    """
    
    def __init__(self, settings=None, policies=None):
        self.settings = settings or {}
        self.policies = policies or {}
        self.client = HttpClient(settings=settings, policies=policies)
        
        # Brave API for website discovery
        api_key = (self.settings.get("api_keys") or {}).get("brave")
        self.website_discovery = WebsiteDiscovery(
            api_key,
            settings=self.settings.get("enrichment", {}).get("website_discovery", {}),
            http_settings=self.settings,
            policies=self.policies,
        ) if api_key else None
        
        # Contact enricher
        self.contact_enricher = ContactEnricher(
            settings=self.settings,
            policies=self.policies,
            contact_cfg=self.settings.get("enrichment", {}).get("contact", {})
        )
        
        # Stats
        self.stats = {
            "total_processed": 0,
            "needs_enrichment": 0,
            "website_found": 0,
            "email_found": 0,
            "phone_found": 0,
            "failed": 0
        }
    
    def process(self, leads: List[Dict], max_enrichments: int = 100) -> Tuple[List[Dict], Dict]:
        """
        Process leads and enrich those without proper websites.
        
        Args:
            leads: List of lead dictionaries
            max_enrichments: Maximum number of leads to enrich (rate limiting)
            
        Returns:
            Tuple of (enriched_leads, stats)
        """
        logger.info("=" * 60)
        logger.info("🔍 ENRICHMENT QUEUE PROCESSING")
        logger.info("=" * 60)
        
        enriched_leads = []
        enrichment_count = 0
        
        for lead in leads:
            self.stats["total_processed"] += 1
            
            # Check if lead needs enrichment
            needs_enrichment, reason = self._needs_enrichment(lead)
            
            if needs_enrichment and enrichment_count < max_enrichments:
                self.stats["needs_enrichment"] += 1
                
                logger.debug(f"Enriching: {lead.get('company')} - {reason}")
                
                # Try to enrich
                enriched = self._enrich_lead(lead)
                
                if enriched.get("website") and self._is_valid_website(enriched.get("website", "")):
                    self.stats["website_found"] += 1
                
                if enriched.get("emails"):
                    self.stats["email_found"] += 1
                
                if enriched.get("phones"):
                    self.stats["phone_found"] += 1
                
                enriched["enrichment_status"] = "enriched"
                enriched["enrichment_reason"] = reason
                enriched_leads.append(enriched)
                enrichment_count += 1
            else:
                # Keep lead as is
                lead["enrichment_status"] = "not_needed" if not needs_enrichment else "skipped"
                enriched_leads.append(lead)
        
        # Log summary
        logger.info(f"\n📊 Enrichment Summary:")
        logger.info(f"  Total processed: {self.stats['total_processed']}")
        logger.info(f"  Needed enrichment: {self.stats['needs_enrichment']}")
        logger.info(f"  Website found: {self.stats['website_found']}")
        logger.info(f"  Email found: {self.stats['email_found']}")
        logger.info(f"  Phone found: {self.stats['phone_found']}")
        
        return enriched_leads, self.stats
    
    def _needs_enrichment(self, lead: Dict) -> Tuple[bool, str]:
        """Check if lead needs enrichment."""
        website = lead.get("website", "")
        
        # Handle NaN/None values
        if not website or str(website).lower() in {"nan", "none", "", "[]"}:
            return True, "no_website"
        
        # Check if domain is non-company
        try:
            parsed = urlparse(str(website))
            domain = parsed.netloc.lower().replace("www.", "")
            
            # Check against non-company domains
            for non_company in NON_COMPANY_DOMAINS:
                if non_company in domain:
                    return True, f"non_company_domain:{domain}"
        except:
            return True, "invalid_url"
        
        # Check for emails - if no email, still try to enrich
        emails = lead.get("emails", [])
        if not emails or (isinstance(emails, str) and emails.lower() in {"nan", "none", "[]", ""}):
            return True, "no_email"
        
        return False, ""
    
    def _is_valid_website(self, website: str) -> bool:
        """Check if website is valid (not a non-company domain)."""
        if not website:
            return False
        
        try:
            parsed = urlparse(str(website))
            domain = parsed.netloc.lower().replace("www.", "")
            
            for non_company in NON_COMPANY_DOMAINS:
                if non_company in domain:
                    return False
            
            return True
        except:
            return False
    
    def _enrich_lead(self, lead: Dict) -> Dict:
        """Enrich a single lead with website discovery and contact extraction."""
        company = lead.get("company", "")
        country = lead.get("country", "")
        
        if not company:
            self.stats["failed"] += 1
            return lead
        
        # Step 1: Try website discovery via Brave Search
        if self.website_discovery:
            # Build search query
            query_parts = [company]
            if country:
                query_parts.append(country)
            query_parts.append("textile")
            
            search_query = " ".join(query_parts)
            
            try:
                discovered = self.website_discovery.search_website(search_query, company)
                
                if discovered and self._is_valid_website(discovered):
                    lead["website"] = discovered
                    lead["website_source"] = "brave_enrichment"
                    logger.debug(f"  Found website: {discovered}")
            except Exception as e:
                logger.warning(f"Website discovery failed for {company}: {e}")
        
        # Step 2: Extract contacts from website
        if lead.get("website") and self._is_valid_website(lead.get("website", "")):
            try:
                lead = self.contact_enricher.enrich(lead)
            except Exception as e:
                logger.warning(f"Contact extraction failed for {company}: {e}")
        
        return lead
    
    def get_enrichment_queue(self, leads: List[Dict]) -> List[Dict]:
        """Get list of leads that need enrichment without processing them."""
        queue = []
        
        for lead in leads:
            needs_enrichment, reason = self._needs_enrichment(lead)
            if needs_enrichment:
                lead_copy = lead.copy()
                lead_copy["enrichment_reason"] = reason
                queue.append(lead_copy)
        
        return queue


# Test
if __name__ == "__main__":
    import yaml
    
    # Load settings
    with open("config/settings.yaml") as f:
        settings = yaml.safe_load(f)
    
    # Sample leads
    test_leads = [
        {
            "company": "Santana Textiles",
            "country": "Brazil",
            "website": "https://abit.org.br/company/santana",  # Wrong - association site
            "emails": []
        },
        {
            "company": "Döhler Textil",
            "country": "Brazil",
            "website": "",  # Missing
            "emails": []
        },
        {
            "company": "Vicunha",
            "country": "Brazil",
            "website": "https://www.vicunha.com.br",  # Correct
            "emails": ["contato@vicunha.com.br"]
        }
    ]
    
    queue = EnrichmentQueue(settings=settings)
    
    # Just check what needs enrichment
    needs_enrichment = queue.get_enrichment_queue(test_leads)
    print(f"\nLeads needing enrichment: {len(needs_enrichment)}")
    for lead in needs_enrichment:
        print(f"  - {lead['company']}: {lead['enrichment_reason']}")
    
    # Process (with Brave API)
    # enriched, stats = queue.process(test_leads, max_enrichments=10)
    # print(f"\nStats: {stats}")
