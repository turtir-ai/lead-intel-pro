#!/usr/bin/env python3
"""
Lead Role Classifier - Classify leads as Mill/Customer vs Dealer/Supplier
Based on project_v4.md requirements

Goal: Identify leads that are actual CUSTOMERS (textile mills, dyehouses, finishers)
vs intermediaries (dealers, distributors, spare parts shops, news sites)
"""

import re
import logging
from typing import Dict, List, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RoleScore:
    """Role classification result."""
    role: str           # CUSTOMER, SUPPLIER, INTERMEDIARY, UNKNOWN
    confidence: float   # 0.0 to 1.0
    positive_signals: List[str]
    negative_signals: List[str]


class LeadRoleClassifier:
    """
    Classifies leads by their role in the stenter spare parts value chain.
    
    Target Roles (CUSTOMER - we want these):
    - Textile Mill / Dyehouse / Finisher / Integrated textile group
    - Uses stenter machines for fabric finishing
    
    Non-Target Roles (INTERMEDIARY - filter out):
    - Machine dealer / distributor
    - Spare parts shop / broker
    - News site / magazine
    - Job board / recruitment
    """
    
    # Positive signals (indicates CUSTOMER - textile producer)
    CUSTOMER_KEYWORDS = {
        # Processing activities (what they DO)
        'dyeing', 'finishing', 'bleaching', 'printing', 'coating',
        'sanforizing', 'mercerizing', 'stentering', 'tentering',
        'wet processing', 'fabric processing', 'textile processing',
        
        # Turkish equivalents
        'boya', 'terbiye', 'apre', 'baski', 'kaplama', 'yikama',
        
        # Facility types
        'mill', 'dyehouse', 'dye house', 'finishing plant', 'finishing house',
        'textile mill', 'fabric mill', 'cotton mill', 'weaving mill',
        'processing plant', 'production plant', 'manufacturing plant',
        
        # Turkish facilities
        'fabrika', 'tesisi', 'üretim', 'imalat', 'dokuma', 'örme',
        
        # Products they make (end products)
        'denim', 'cotton fabric', 'polyester fabric', 'knitted fabric',
        'woven fabric', 'home textile', 'technical textile', 'workwear',
        'shirting', 'suiting', 'upholstery', 'curtain', 'towel',
        
        # Equipment they USE (not sell)
        'stenter machine', 'ram makine', 'finishing range', 'dyeing range',
        'jigger', 'jet dyeing', 'pad steam', 'continuous dyeing',
    }
    
    # Strong positive signals (high confidence customer)
    STRONG_CUSTOMER_SIGNALS = {
        'integrated textile', 'vertical integration', 'spinning to finishing',
        'yarn to fabric', 'weaving and finishing', 'dyeing and finishing',
        'fabric manufacturer', 'textile manufacturer', 'fabric producer',
        'finishing capacity', 'dyeing capacity', 'production capacity',
        'oeko-tex certified', 'gots certified', 'iso certified',
        'export to europe', 'supply to brands', 'customer brands',
    }
    
    # Negative signals (indicates INTERMEDIARY - not our customer)
    INTERMEDIARY_KEYWORDS = {
        # Dealer/distributor
        'dealer', 'distributor', 'trading', 'trader', 'importer', 'exporter',
        'agent', 'broker', 'reseller', 'wholesale', 'retailer',
        
        # Spare parts (competitor or same business as us)
        'spare parts', 'spareparts', 'yedek parça', 'parts supplier',
        'machine parts', 'replacement parts', 'accessories',
        
        # Machine seller (not user)
        'machine sales', 'machine dealer', 'equipment dealer',
        'machinery trading', 'selling machines', 'buy machines',
        
        # News/media
        'news', 'magazine', 'journal', 'publication', 'media',
        'press release', 'announcement', 'article', 'blog',
        
        # Job/recruitment
        'job', 'career', 'vacancy', 'hiring', 'recruitment', 'employment',
        
        # Marketplace
        'marketplace', 'b2b portal', 'directory listing', 'classifieds',
    }
    
    # Strong negative signals (definitely not customer)
    STRONG_INTERMEDIARY_SIGNALS = {
        'spare parts shop', 'parts supplier', 'machine trading company',
        'textile machinery dealer', 'equipment trading', 'machinery sales',
        'buy and sell', 'import export trading', 'trading house',
        'news portal', 'industry magazine', 'textile news',
    }
    
    # Source type weights
    SOURCE_WEIGHTS = {
        # High value (likely customer) - V5 updated
        'known_manufacturer': 1.0,  # Highest - pre-verified from config
        'oem_customer': 0.9,
        'gots': 0.8,              # GOTS certified = real producer
        'oekotex': 0.8,           # OEKO-TEX certified = real producer
        'fair_exhibitor': 0.6,    # Could be customer or supplier
        'facility_verified': 0.8,
        
        # Medium value
        'directory': 0.5,
        'association_member': 0.6,
        'precision_search': 0.6,
        
        # Low value (mixed)
        'brave_search': 0.3,
        'web_scrape': 0.2,
    }
    
    def __init__(self):
        self.stats = {'CUSTOMER': 0, 'INTERMEDIARY': 0, 'UNKNOWN': 0}
    
    def classify(self, lead: Dict) -> RoleScore:
        """
        Classify a lead's role.
        
        Args:
            lead: Dictionary with company, source_type, context, website, etc.
            
        Returns:
            RoleScore with role, confidence, and signals
        """
        company = str(lead.get('company', '')).lower()
        source_type = str(lead.get('source_type', '')).lower()
        context = str(lead.get('context', lead.get('description', ''))).lower()
        website = str(lead.get('website', '')).lower()
        
        # Combine all text for analysis
        all_text = f"{company} {context} {website}"
        
        positive_signals = []
        negative_signals = []
        score = 0.0
        
        # 1. Check strong signals first
        for signal in self.STRONG_CUSTOMER_SIGNALS:
            if signal in all_text:
                positive_signals.append(f"STRONG: {signal}")
                score += 0.3
        
        for signal in self.STRONG_INTERMEDIARY_SIGNALS:
            if signal in all_text:
                negative_signals.append(f"STRONG: {signal}")
                score -= 0.4
        
        # 2. Check regular keywords
        for keyword in self.CUSTOMER_KEYWORDS:
            if keyword in all_text:
                positive_signals.append(keyword)
                score += 0.1
        
        for keyword in self.INTERMEDIARY_KEYWORDS:
            if keyword in all_text:
                negative_signals.append(keyword)
                score -= 0.15
        
        # 3. Source type weight
        source_weight = self.SOURCE_WEIGHTS.get(source_type, 0.3)
        score += (source_weight - 0.5) * 0.5  # Adjust score by source quality
        
        if source_weight >= 0.7:
            positive_signals.append(f"source:{source_type}")
        elif source_weight <= 0.3:
            negative_signals.append(f"source:{source_type}")
        
        # 4. Determine role and confidence
        if score >= 0.3:
            role = 'CUSTOMER'
            confidence = min(0.9, 0.5 + score)
        elif score <= -0.2:
            role = 'INTERMEDIARY'
            confidence = min(0.9, 0.5 - score)
        else:
            role = 'UNKNOWN'
            confidence = 0.3
        
        self.stats[role] += 1
        
        return RoleScore(
            role=role,
            confidence=round(confidence, 2),
            positive_signals=positive_signals[:5],  # Top 5
            negative_signals=negative_signals[:5]
        )
    
    def classify_leads(self, leads: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """
        Classify all leads into customer/intermediary/unknown.
        
        Returns:
            Tuple of (customers, intermediaries, unknown)
        """
        customers = []
        intermediaries = []
        unknown = []
        
        for lead in leads:
            result = self.classify(lead)
            
            # Add classification to lead
            lead['role'] = result.role
            lead['role_confidence'] = result.confidence
            lead['role_positive'] = '; '.join(result.positive_signals)
            lead['role_negative'] = '; '.join(result.negative_signals)
            
            if result.role == 'CUSTOMER':
                customers.append(lead)
            elif result.role == 'INTERMEDIARY':
                intermediaries.append(lead)
            else:
                unknown.append(lead)
        
        logger.info(f"Role Classification: CUSTOMER={len(customers)}, "
                   f"INTERMEDIARY={len(intermediaries)}, UNKNOWN={len(unknown)}")
        
        return customers, intermediaries, unknown
    
    def filter_customers_only(self, leads: List[Dict], include_unknown: bool = True) -> List[Dict]:
        """
        Filter leads to keep only customers (and optionally unknown).
        
        Args:
            leads: List of lead dictionaries
            include_unknown: If True, include UNKNOWN role leads for manual review
            
        Returns:
            List of leads that are likely customers
        """
        customers, intermediaries, unknown = self.classify_leads(leads)
        
        if include_unknown:
            result = customers + unknown
            logger.info(f"Keeping {len(result)} leads (customers + unknown)")
        else:
            result = customers
            logger.info(f"Keeping {len(result)} customer leads only")
        
        return result


def classify_lead_role(lead: Dict) -> str:
    """Convenience function to classify a single lead."""
    classifier = LeadRoleClassifier()
    result = classifier.classify(lead)
    return result.role


if __name__ == '__main__':
    # Test with sample data
    test_leads = [
        {
            'company': 'ABC Tekstil A.Ş.',
            'source_type': 'gots',
            'context': 'Dyeing and finishing facility with stenter machines'
        },
        {
            'company': 'XYZ Machine Trading',
            'source_type': 'brave_search',
            'context': 'Textile machinery dealer and spare parts supplier'
        },
        {
            'company': 'Textile News Daily',
            'source_type': 'brave_search',
            'context': 'Latest news from the textile industry'
        },
        {
            'company': 'Korteks Mensucat',
            'source_type': 'oem_customer',
            'context': 'Integrated textile producer with finishing plant'
        },
        {
            'company': 'Unknown Company',
            'source_type': 'directory',
            'context': ''
        },
    ]
    
    logging.basicConfig(level=logging.INFO)
    classifier = LeadRoleClassifier()
    customers, intermediaries, unknown = classifier.classify_leads(test_leads)
    
    print("\n=== CUSTOMERS ===")
    for lead in customers:
        print(f"  [{lead['role_confidence']}] {lead['company']} - {lead['role_positive']}")
    
    print("\n=== INTERMEDIARIES ===")
    for lead in intermediaries:
        print(f"  [{lead['role_confidence']}] {lead['company']} - {lead['role_negative']}")
    
    print("\n=== UNKNOWN ===")
    for lead in unknown:
        print(f"  {lead['company']}")
