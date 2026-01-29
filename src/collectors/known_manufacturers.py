#!/usr/bin/env python3
"""
Known Manufacturers Collector - Generate leads from targets.yaml known_manufacturers
These are verified stenter customers from config
"""

import logging
from typing import List, Dict
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


class KnownManufacturersCollector:
    """
    Generates leads from known manufacturers defined in targets.yaml.
    These are pre-verified companies that use stenter machines.
    """
    
    def __init__(self, targets_config: Dict = None, config_path: str = None):
        if targets_config:
            self.config = targets_config
        elif config_path:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            config_path = Path(__file__).parents[2] / "config" / "targets.yaml"
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
    
    def harvest(self, regions: List[str] = None) -> List[Dict]:
        """
        Harvest leads from known_manufacturers in targets.yaml.
        
        Args:
            regions: Optional list of region keys to include (e.g., ['south_america', 'turkey'])
                    If None, include all regions.
        
        Returns:
            List of lead dictionaries
        """
        leads = []
        
        # Region keys to check
        region_keys = regions or [
            'south_america', 'north_africa', 'south_asia', 'turkey', 'other_markets'
        ]
        
        for region_key in region_keys:
            region = self.config.get(region_key, {})
            if not region:
                continue
            
            region_label = region.get('region_label', region_key.replace('_', ' ').title())
            countries = region.get('countries', {})
            
            if not isinstance(countries, dict):
                continue
            
            for country_key, country_data in countries.items():
                if not isinstance(country_data, dict):
                    continue
                
                country_labels = country_data.get('labels', [])
                country_name = country_labels[0] if country_labels else country_key.title()
                country_code = country_data.get('code', '')
                
                known_manufacturers = country_data.get('known_manufacturers', [])
                
                for company in known_manufacturers:
                    leads.append({
                        'company': company,
                        'country': country_name,
                        'country_code': country_code,
                        'region': region_label,
                        'source_type': 'known_manufacturer',
                        'source_name': 'targets.yaml',
                        'source': f'config:targets.yaml:{region_key}:{country_key}',
                        'context': f"Known stenter customer in {country_name}. Pre-verified manufacturer from targets configuration.",
                        'brand_mentioned': True,
                        'has_textile_context': True,
                    })
        
        logger.info(f"KnownManufacturersCollector: Generated {len(leads)} leads from config")
        return leads
    
    def harvest_by_country(self, country_code: str) -> List[Dict]:
        """
        Harvest leads for a specific country code (e.g., 'BRA', 'TUR').
        """
        all_leads = self.harvest()
        return [lead for lead in all_leads if lead.get('country_code') == country_code]
    
    def harvest_south_america(self) -> List[Dict]:
        """
        Harvest leads specifically for South America.
        """
        return self.harvest(regions=['south_america'])


if __name__ == "__main__":
    # Test
    import os
    os.chdir('/Users/dev/Documents/germany/lead_intel_v2')
    
    collector = KnownManufacturersCollector()
    leads = collector.harvest()
    
    print(f"Total known manufacturers: {len(leads)}")
    
    # Count by country
    from collections import Counter
    countries = Counter(lead['country'] for lead in leads)
    print("\nBy country:")
    for country, count in countries.most_common():
        print(f"  {country}: {count}")
    
    # Show South America
    sa_leads = collector.harvest_south_america()
    print(f"\nSouth America: {len(sa_leads)}")
    for lead in sa_leads[:10]:
        print(f"  - {lead['company']} ({lead['country']})")
