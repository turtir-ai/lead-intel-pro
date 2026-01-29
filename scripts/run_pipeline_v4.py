#!/usr/bin/env python3
"""
Pipeline v4 - Advanced lead qualification based on project_v4.md
Improvements:
- Entity Quality Gate v2 (title/sentence/person detection)
- Lead Role Classification (Customer vs Intermediary)
- Association Members harvesting
- Evidence-based scoring
- HS code integration
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.processors.entity_quality_gate_v2 import EntityQualityGateV2
from src.processors.lead_role_classifier import LeadRoleClassifier
from src.processors.dedupe import LeadDedupe

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Brave API Key
BRAVE_API_KEY = os.environ.get('BRAVE_API_KEY', 'BSAYTcCa5ZtcjOYZCEduotyNwmZVRXa')


class PipelineV4:
    """
    Advanced lead qualification pipeline based on project_v4.md.
    """
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.data_dir = project_root / 'data'
        self.output_dir = project_root / 'outputs' / 'crm'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Processors
        self.quality_gate = EntityQualityGateV2()
        self.role_classifier = LeadRoleClassifier()
        
        # Statistics
        self.stats = {
            'input_leads': 0,
            'after_quality_gate': 0,
            'customers': 0,
            'intermediaries': 0,
            'unknown': 0,
            'after_dedupe': 0,
            'exported': 0,
        }
    
    def run(self, input_file: Path = None):
        """Run the full v4 pipeline."""
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("PIPELINE V4 - Advanced Lead Qualification")
        logger.info("=" * 60)
        
        # 1. Load leads
        if input_file is None:
            input_file = self.data_dir / 'staging' / 'leads_raw.csv'
        
        leads = self._load_leads(input_file)
        self.stats['input_leads'] = len(leads)
        logger.info(f"Loaded {len(leads)} leads from {input_file.name}")
        
        # 2. Apply Entity Quality Gate v2
        logger.info("\n--- STAGE: Entity Quality Gate v2 ---")
        leads = self.quality_gate.process_leads(leads)
        self.stats['after_quality_gate'] = len(leads)
        
        # Show rejection breakdown
        gate_stats = self.quality_gate.get_stats()
        logger.info(f"Rejection reasons:")
        for reason, count in sorted(gate_stats['rejection_reasons'].items(), key=lambda x: -x[1])[:10]:
            logger.info(f"  {reason}: {count}")
        
        # 3. Apply Role Classification
        logger.info("\n--- STAGE: Lead Role Classification ---")
        customers, intermediaries, unknown = self.role_classifier.classify_leads(leads)
        
        self.stats['customers'] = len(customers)
        self.stats['intermediaries'] = len(intermediaries)
        self.stats['unknown'] = len(unknown)
        
        logger.info(f"  CUSTOMER: {len(customers)}")
        logger.info(f"  INTERMEDIARY: {len(intermediaries)} (filtered out)")
        logger.info(f"  UNKNOWN: {len(unknown)}")
        
        # Keep customers + unknown for now
        qualified_leads = customers + unknown
        
        # 4. Deduplicate
        logger.info("\n--- STAGE: Deduplication ---")
        deduplicator = LeadDedupe()
        unique_leads, audit = deduplicator.dedupe(qualified_leads)
        self.stats['after_dedupe'] = len(unique_leads)
        logger.info(f"  After dedupe: {len(unique_leads)} unique leads")
        
        # 5. Score and rank
        logger.info("\n--- STAGE: Scoring ---")
        scored_leads = self._score_leads(unique_leads)
        scored_leads.sort(key=lambda x: x.get('v4_score', 0), reverse=True)
        
        # 6. Export tiered outputs
        logger.info("\n--- STAGE: Export ---")
        self._export_results(scored_leads)
        self.stats['exported'] = len(scored_leads)
        
        # Summary
        duration = (datetime.now() - start_time).seconds
        self._print_summary(scored_leads, duration)
        
        return scored_leads
    
    def _load_leads(self, file_path: Path) -> List[Dict]:
        """Load leads from CSV."""
        df = pd.read_csv(file_path)
        return df.to_dict('records')
    
    def _score_leads(self, leads: List[Dict]) -> List[Dict]:
        """
        Score leads based on v4 criteria:
        - Evidence quality
        - Source reliability
        - Role confidence
        - Company indicators
        """
        for lead in leads:
            score = 0
            score_reasons = []
            
            # 1. Entity grade bonus
            grade = lead.get('entity_grade', 'C')
            grade_scores = {'A': 30, 'B': 20, 'C': 10}
            score += grade_scores.get(grade, 0)
            if grade in grade_scores:
                score_reasons.append(f"Grade {grade}")
            
            # 2. Role classification bonus
            role = lead.get('role', 'UNKNOWN')
            role_confidence = lead.get('role_confidence', 0)
            if role == 'CUSTOMER':
                score += int(role_confidence * 40)
                score_reasons.append(f"Customer:{role_confidence}")
            elif role == 'UNKNOWN':
                score += 10
            
            # 3. Source type bonus
            source_type = str(lead.get('source_type', '')).lower()
            source_scores = {
                'oem_customer': 50,
                'known_manufacturer': 40,
                'facility_verified': 35,
                'gots': 25,
                'oekotex': 25,
                'association_member': 20,
                'fair_exhibitor': 15,
                'precision_search': 15,
                'directory': 10,
                'brave_search': 5,
            }
            if source_type in source_scores:
                score += source_scores[source_type]
                score_reasons.append(f"Source:{source_type}")
            
            # 4. Evidence bonus
            if lead.get('website') and str(lead.get('website')).lower() not in ('nan', 'none', ''):
                score += 10
                score_reasons.append("Has website")
            
            if lead.get('evidence_url') and str(lead.get('evidence_url')).lower() not in ('nan', 'none', ''):
                score += 10
                score_reasons.append("Has evidence")
            
            if lead.get('email') and str(lead.get('email')).lower() not in ('nan', 'none', '', '[]'):
                score += 5
                score_reasons.append("Has email")
            
            # 5. Finishing keywords bonus
            context = str(lead.get('context', '')).lower()
            company = str(lead.get('company', '')).lower()
            all_text = f"{context} {company}"
            
            finishing_keywords = ['finishing', 'dyeing', 'stenter', 'tenter', 'bleaching', 
                                 'terbiye', 'boya', 'brückner', 'monforts']
            keyword_hits = sum(1 for kw in finishing_keywords if kw in all_text)
            if keyword_hits > 0:
                score += min(keyword_hits * 5, 20)
                score_reasons.append(f"Keywords:{keyword_hits}")
            
            lead['v4_score'] = score
            lead['v4_score_reasons'] = '; '.join(score_reasons)
            
            # Determine tier
            if score >= 80:
                lead['v4_tier'] = 'TIER1-Premium'
            elif score >= 60:
                lead['v4_tier'] = 'TIER2-High'
            elif score >= 40:
                lead['v4_tier'] = 'TIER3-Medium'
            elif score >= 20:
                lead['v4_tier'] = 'TIER4-Low'
            else:
                lead['v4_tier'] = 'TIER5-Review'
        
        return leads
    
    def _export_results(self, leads: List[Dict]):
        """Export tiered results to CSV files."""
        df = pd.DataFrame(leads)
        
        # Column order for output
        priority_cols = [
            'company', 'country', 'v4_score', 'v4_tier', 'role', 'role_confidence',
            'entity_grade', 'source_type', 'website', 'email', 'phone',
            'evidence_url', 'v4_score_reasons'
        ]
        
        available_cols = [c for c in priority_cols if c in df.columns]
        other_cols = [c for c in df.columns if c not in priority_cols]
        final_cols = available_cols + other_cols
        df = df[final_cols]
        
        # Export all leads
        all_file = self.output_dir / 'v4_all_leads.csv'
        df.to_csv(all_file, index=False)
        logger.info(f"  Exported {len(df)} leads to v4_all_leads.csv")
        
        # Export by tier
        tier_counts = {}
        for tier in ['TIER1-Premium', 'TIER2-High', 'TIER3-Medium', 'TIER4-Low', 'TIER5-Review']:
            tier_df = df[df['v4_tier'] == tier]
            if len(tier_df) > 0:
                tier_file = self.output_dir / f"v4_{tier.lower().replace('-', '_')}.csv"
                tier_df.to_csv(tier_file, index=False)
                tier_counts[tier] = len(tier_df)
        
        logger.info(f"  Tier distribution: {tier_counts}")
        
        # Export top 100
        top100 = df.head(100)
        top100.to_csv(self.output_dir / 'v4_top100.csv', index=False)
        logger.info(f"  Exported top 100 leads")
        
        # Export customers only (for sales)
        customers_df = df[df['role'] == 'CUSTOMER']
        customers_df.to_csv(self.output_dir / 'v4_customers_only.csv', index=False)
        logger.info(f"  Exported {len(customers_df)} customer-role leads")
    
    def _print_summary(self, leads: List[Dict], duration: int):
        """Print pipeline summary."""
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE V4 COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Duration: {duration} seconds")
        logger.info("")
        logger.info("Statistics:")
        for key, value in self.stats.items():
            logger.info(f"  {key}: {value}")
        
        # Tier breakdown
        df = pd.DataFrame(leads)
        if 'v4_tier' in df.columns:
            logger.info("\nTier Distribution:")
            tier_counts = df['v4_tier'].value_counts()
            for tier, count in tier_counts.items():
                pct = count / len(df) * 100
                logger.info(f"  {tier}: {count} ({pct:.1f}%)")
        
        # Country breakdown
        if 'country' in df.columns:
            logger.info("\nTop Countries:")
            country_counts = df['country'].value_counts().head(10)
            for country, count in country_counts.items():
                logger.info(f"  {country}: {count}")
        
        # Sample high-quality leads
        logger.info("\nTop 10 Leads:")
        for lead in leads[:10]:
            company = lead.get('company', 'Unknown')[:40]
            score = lead.get('v4_score', 0)
            tier = lead.get('v4_tier', 'Unknown')
            country = lead.get('country', 'Unknown')
            logger.info(f"  [{score}] {company} | {country} | {tier}")


def main():
    """Run pipeline v4."""
    project_root = Path(__file__).parent.parent
    
    # Check if specific input file provided
    input_file = None
    if len(sys.argv) > 1:
        input_file = Path(sys.argv[1])
    
    pipeline = PipelineV4(project_root)
    leads = pipeline.run(input_file)
    
    return leads


if __name__ == '__main__':
    main()
