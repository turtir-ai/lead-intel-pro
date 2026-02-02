"""
Entity Validator Module - Phase 1: Data Quality Foundation
Classifies entities by business role (End-User, Intermediary, Brand)
"""

from enum import Enum
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class EntityType(Enum):
    """Business role classification"""
    END_USER = "end_user"          # Target: Textile mills buying parts
    INTERMEDIARY = "intermediary"  # Machine sellers/manufacturers
    BRAND = "brand"                # Fashion brands (not direct buyers)
    ASSOCIATION = "association"    # Trade associations/chambers
    UNKNOWN = "unknown"


class EntityValidator:
    """
    Classify entities by business role
    """
    
    # Keywords for each entity type
    END_USER_KEYWORDS = [
        # English
        "dyeing", "textile mill", "fabric processing", "finishing",
        "knitting", "weaving", "spinning", "garment manufacturer",
        "fabric producer", "textile factory", "processing plant",
        "dyehouse", "print house", "textile processing",
        # Turkish
        "terbiye", "boyahane", "örme", "dokuma", "iplik",
        # Spanish/Portuguese
        "tintura", "teñido", "acabado", "tejeduría"
    ]
    
    INTERMEDIARY_KEYWORDS = [
        # Machine sellers/manufacturers
        "machinery", "equipment", "technology", "manufacturer",
        "machine seller", "stenter manufacturer", "textile machinery",
        "equipment supplier", "machine supplier",
        # Known brands
        "brückner", "lafer", "monforts", "babcock", "benninger",
        "morrison", "santex", "ferraro", "goller", "krantz",
        "thies", "then", "mathis", "reggiani", "arioli"
    ]
    
    BRAND_KEYWORDS = [
        "fashion", "apparel", "clothing", "garment brand",
        "retail", "design house", "collection", "boutique",
        "clothing brand", "fashion house"
    ]
    
    ASSOCIATION_KEYWORDS = [
        "association", "chamber", "council", "federation",
        "institute", "society", "union", "syndicate",
        "dernek", "birlik", "oda", "kurum"  # Turkish
    ]
    
    # Machine seller company name patterns
    MACHINE_COMPANY_PATTERNS = [
        r"machinery", r"equipment", r"machine", r"tech(nology)?",
        r"engineering", r"industrial", r"systems"
    ]
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize validator with optional config
        
        Args:
            config: Optional dict with additional keywords
        """
        if config:
            self.END_USER_KEYWORDS.extend(config.get('end_user_keywords', []))
            self.INTERMEDIARY_KEYWORDS.extend(config.get('intermediary_keywords', []))
    
    def classify_entity(self, company_name: str, description: str = "", 
                       website_content: str = "") -> EntityType:
        """
        Determine entity type from name and description
        
        Priority:
        1. ASSOCIATION (exclude first - not customers)
        2. INTERMEDIARY (exclude second - not our customers)
        3. END_USER (main target)
        4. BRAND (secondary - may outsource)
        
        Args:
            company_name: Company name
            description: Company description or activity
            website_content: Content from website
            
        Returns:
            EntityType enum value
        """
        text = f"{company_name} {description} {website_content}".lower()
        
        # Check Association (highest priority to exclude)
        if any(kw in text for kw in self.ASSOCIATION_KEYWORDS):
            logger.debug(f"Classified {company_name} as ASSOCIATION")
            return EntityType.ASSOCIATION
        
        # Check Intermediary (second priority to exclude)
        if any(kw in text for kw in self.INTERMEDIARY_KEYWORDS):
            logger.debug(f"Classified {company_name} as INTERMEDIARY")
            return EntityType.INTERMEDIARY
        
        # Check End-User (main target)
        if any(kw in text for kw in self.END_USER_KEYWORDS):
            logger.debug(f"Classified {company_name} as END_USER")
            return EntityType.END_USER
        
        # Check Brand
        if any(kw in text for kw in self.BRAND_KEYWORDS):
            logger.debug(f"Classified {company_name} as BRAND")
            return EntityType.BRAND
        
        logger.debug(f"Could not classify {company_name}, marked as UNKNOWN")
        return EntityType.UNKNOWN
    
    def should_process(self, entity_type: EntityType) -> bool:
        """
        Determine if entity should continue through pipeline
        
        Process: END_USER, BRAND (maybe), UNKNOWN (investigate)
        Skip: INTERMEDIARY, ASSOCIATION
        
        Args:
            entity_type: EntityType enum
            
        Returns:
            True if should process, False if should skip
        """
        skip_types = [EntityType.INTERMEDIARY, EntityType.ASSOCIATION]
        return entity_type not in skip_types
    
    def get_priority_score(self, entity_type: EntityType) -> int:
        """
        Get priority score for sorting
        
        Higher score = higher priority
        
        Args:
            entity_type: EntityType enum
            
        Returns:
            Priority score (0-100)
        """
        priority_map = {
            EntityType.END_USER: 100,
            EntityType.BRAND: 60,
            EntityType.UNKNOWN: 40,
            EntityType.INTERMEDIARY: 10,
            EntityType.ASSOCIATION: 0
        }
        return priority_map.get(entity_type, 0)
    
    def validate_entity(self, lead: Dict) -> Dict:
        """
        Classify and validate a single lead
        
        Args:
            lead: Lead dict with 'company' or 'company_name', description, etc.
            
        Returns:
            Lead dict with entity_type and should_process fields
        """
        # Handle both 'company' and 'company_name' fields
        company_name = lead.get('company_name') or lead.get('company', '')
        
        entity_type = self.classify_entity(
            company_name=company_name,
            description=lead.get('description', '') or lead.get('context', ''),
            website_content=lead.get('website_content', '')
        )
        
        lead['entity_type'] = entity_type.value
        lead['should_process'] = self.should_process(entity_type)
        lead['priority_score'] = self.get_priority_score(entity_type)
        
        return lead
    
    def batch_validate(self, leads: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """
        Validate multiple leads at once
        
        Args:
            leads: List of lead dicts
            
        Returns:
            Tuple of (processable_leads, skipped_leads)
        """
        processable = []
        skipped = []
        
        for lead in leads:
            validated = self.validate_entity(lead)
            
            if validated['should_process']:
                processable.append(validated)
            else:
                skipped.append({
                    **validated,
                    'skip_reason': f"entity_type_{validated['entity_type']}"
                })
        
        logger.info(f"Validated {len(leads)} leads: "
                   f"{len(processable)} processable, {len(skipped)} skipped")
        
        return processable, skipped
    
    def get_distribution(self, leads: List[Dict]) -> Dict:
        """
        Get entity type distribution statistics
        
        Args:
            leads: List of leads with entity_type field
            
        Returns:
            Dict with counts per entity type
        """
        distribution = {et.value: 0 for et in EntityType}
        
        for lead in leads:
            entity_type = lead.get('entity_type', 'unknown')
            if entity_type in distribution:
                distribution[entity_type] += 1
        
        total = len(leads)
        percentages = {
            k: round(v / total * 100, 1) if total > 0 else 0 
            for k, v in distribution.items()
        }
        
        return {
            'counts': distribution,
            'percentages': percentages,
            'total': total
        }
