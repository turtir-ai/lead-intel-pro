#!/usr/bin/env python3
"""
Global Customer Finder - Orchestrates all intelligence sources to find real customers
Implements skill: global-customer-finder
"""

import os
import logging
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime
import yaml
import pandas as pd

# Import all collectors
from src.collectors.oem_reference_extractor import OEMReferenceExtractor
from src.collectors.regional_collector import RegionalCollector

# Import processors
from src.processors.entity_quality_gate import EntityQualityGate
from src.processors.customer_qualifier import CustomerQualifier

from src.utils.logger import get_logger

logger = get_logger(__name__)


class GlobalCustomerFinder:
    """
    Master orchestrator for finding real stenter spare parts customers globally.
    
    Combines:
    - OEM reference extraction (Brückner, Monforts news)
    - Regional collection (South America, North Africa, South Asia)
    - Known manufacturers list
    - Quality filtering (entity gate + customer qualifier)
    
    Output: High-confidence customer list ready for CRM.
    """
    
    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path(__file__).parent.parent.parent
        self.config = self._load_configs()
        
        # Initialize components
        self.oem_extractor = OEMReferenceExtractor()
        self.regional_collector = RegionalCollector(self.config.get('targets', {}))
        self.quality_gate = EntityQualityGate()
        self.customer_qualifier = CustomerQualifier()
        
        # Results storage
        self.all_leads = []
        self.qualified_customers = []
        self.stats = {
            'sources': {},
            'countries': {},
            'quality_grades': {},
            'start_time': None,
            'end_time': None
        }
    
    def _load_configs(self) -> Dict:
        """Load all configuration files."""
        config = {}
        config_dir = self.base_path / 'config'
        
        for config_file in ['targets.yaml', 'sources.yaml', 'products.yaml', 'scoring.yaml']:
            path = config_dir / config_file
            if path.exists():
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        config[config_file.replace('.yaml', '')] = yaml.safe_load(f) or {}
                except Exception as e:
                    logger.warning(f"Failed to load {config_file}: {e}")
        
        return config
    
    def run(self, stages: Optional[List[str]] = None) -> List[Dict]:
        """
        Run the complete customer finding pipeline.
        
        Args:
            stages: Optional list of stages to run. If None, runs all.
                   Options: ['known', 'oem', 'regional', 'fairs', 'directories']
        
        Returns:
            List of qualified customer leads
        """
        self.stats['start_time'] = datetime.now()
        
        if stages is None:
            stages = ['known', 'oem', 'regional']
        
        logger.info("="*60)
        logger.info("🎯 GLOBAL CUSTOMER FINDER - Starting")
        logger.info("="*60)
        
        # Stage 1: Known Manufacturers (100% confidence)
        if 'known' in stages:
            self._collect_known_manufacturers()
        
        # Stage 2: OEM References (95% confidence)
        if 'oem' in stages:
            self._collect_oem_references()
        
        # Stage 3: Regional Collection (varies)
        if 'regional' in stages:
            self._collect_regional()
        
        # Apply quality filtering
        logger.info("\n" + "="*60)
        logger.info("🔍 APPLYING QUALITY FILTERS")
        logger.info("="*60)
        
        # Step 1: Entity Quality Gate
        logger.info(f"Input: {len(self.all_leads)} leads")
        filtered = self.quality_gate.filter_leads(self.all_leads)
        logger.info(f"After Entity Gate: {len(filtered)} leads")
        
        # Step 2: Customer Qualifier
        qualified = []
        for lead in filtered:
            result = self.customer_qualifier.qualify_lead(lead)
            if result.get('is_qualified'):
                lead.update(result)
                qualified.append(lead)
        
        self.qualified_customers = qualified
        logger.info(f"After Customer Qualifier: {len(qualified)} leads")
        
        # Calculate final stats
        self._calculate_stats()
        
        # Save outputs
        self._save_outputs()
        
        self.stats['end_time'] = datetime.now()
        
        # Print summary
        self._print_summary()
        
        return self.qualified_customers
    
    def _collect_known_manufacturers(self):
        """Collect known manufacturers from targets.yaml."""
        logger.info("\n📋 Stage 1: Known Manufacturers")
        
        targets = self.config.get('targets', {})
        count = 0
        
        # Priority regions
        for region_name, region_data in targets.get('priority_regions', {}).items():
            if isinstance(region_data, dict):
                for country_data in region_data.get('countries', []):
                    if isinstance(country_data, dict):
                        country = country_data.get('name', '')
                        for company in country_data.get('known_manufacturers', []):
                            self.all_leads.append({
                                'company': company,
                                'country': country,
                                'source_type': 'known_manufacturer',
                                'source_url': 'config/targets.yaml',
                                'context': f'Known finishing manufacturer in {country}',
                                'confidence': 'high',
                                'region': region_name
                            })
                            count += 1
        
        self.stats['sources']['known_manufacturer'] = count
        logger.info(f"  ✓ Collected {count} known manufacturers")
    
    def _collect_oem_references(self):
        """Collect customers from OEM reference pages."""
        logger.info("\n🏭 Stage 2: OEM References")
        
        sources = self.config.get('sources', {})
        oem_refs = sources.get('oem_references', [])
        
        if not oem_refs:
            logger.info("  ⚠ No OEM reference sources configured")
            return
        
        count = 0
        for oem_source in oem_refs:
            if not oem_source.get('enabled', False):
                continue
            
            base_url = oem_source.get('base_url', '')
            brand = oem_source.get('oem_brand', '')
            paths = oem_source.get('reference_paths', [])
            
            for path in paths:
                url = f"{base_url}{path}"
                logger.info(f"  Extracting from {brand}: {path}")
                
                try:
                    mentions = self.oem_extractor.extract_from_url(url, brand.lower())
                    leads = self.oem_extractor.to_leads(mentions)
                    self.all_leads.extend(leads)
                    count += len(leads)
                except Exception as e:
                    logger.warning(f"  ⚠ Failed to extract from {url}: {e}")
        
        self.stats['sources']['oem_customer'] = count
        logger.info(f"  ✓ Collected {count} OEM customer references")
    
    def _collect_regional(self):
        """Collect leads from priority regions."""
        logger.info("\n🌍 Stage 3: Regional Collection")
        
        try:
            # South America
            sa_leads = self.regional_collector.collect_south_america()
            self.all_leads.extend(sa_leads)
            logger.info(f"  ✓ South America: {len(sa_leads)} leads")
            
            # North Africa
            na_leads = self.regional_collector.collect_north_africa()
            self.all_leads.extend(na_leads)
            logger.info(f"  ✓ North Africa: {len(na_leads)} leads")
            
            # South Asia
            asia_leads = self.regional_collector.collect_south_asia()
            self.all_leads.extend(asia_leads)
            logger.info(f"  ✓ South Asia: {len(asia_leads)} leads")
            
        except Exception as e:
            logger.warning(f"  ⚠ Regional collection error: {e}")
    
    def _calculate_stats(self):
        """Calculate statistics from qualified customers."""
        for lead in self.qualified_customers:
            # By source
            source = lead.get('source_type', 'unknown')
            self.stats['sources'][source] = self.stats['sources'].get(source, 0) + 1
            
            # By country
            country = lead.get('country', 'Unknown')
            self.stats['countries'][country] = self.stats['countries'].get(country, 0) + 1
            
            # By quality grade
            grade = lead.get('entity_quality', 'unknown')
            self.stats['quality_grades'][grade] = self.stats['quality_grades'].get(grade, 0) + 1
    
    def _save_outputs(self):
        """Save qualified customers to CSV."""
        output_dir = self.base_path / 'outputs' / 'crm'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if self.qualified_customers:
            df = pd.DataFrame(self.qualified_customers)
            
            # Save full list
            full_path = output_dir / 'qualified_customers_global.csv'
            df.to_csv(full_path, index=False)
            logger.info(f"\n💾 Saved {len(df)} customers to {full_path}")
            
            # Save top 100 by score
            if 'qualification_score' in df.columns:
                top100 = df.nlargest(100, 'qualification_score')
                top100_path = output_dir / 'top100_global.csv'
                top100.to_csv(top100_path, index=False)
                logger.info(f"💾 Saved top 100 to {top100_path}")
    
    def _print_summary(self):
        """Print final summary."""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        print("\n" + "="*60)
        print("📊 GLOBAL CUSTOMER FINDER - SUMMARY")
        print("="*60)
        print(f"\n⏱  Duration: {duration:.1f} seconds")
        print(f"📥 Total leads collected: {len(self.all_leads)}")
        print(f"✅ Qualified customers: {len(self.qualified_customers)}")
        print(f"📈 Qualification rate: {len(self.qualified_customers)/max(1,len(self.all_leads))*100:.1f}%")
        
        print("\n📦 By Source Type:")
        for source, count in sorted(self.stats['sources'].items(), key=lambda x: -x[1]):
            print(f"   • {source}: {count}")
        
        print("\n🌍 By Country (Top 10):")
        for country, count in sorted(self.stats['countries'].items(), key=lambda x: -x[1])[:10]:
            print(f"   • {country}: {count}")
        
        print("\n⭐ By Quality Grade:")
        for grade, count in sorted(self.stats['quality_grades'].items()):
            print(f"   • Grade {grade}: {count}")
        
        print("\n" + "="*60)


# Standalone execution
if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    finder = GlobalCustomerFinder()
    customers = finder.run()
    
    print(f"\n✅ Found {len(customers)} qualified global customers!")
