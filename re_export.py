#!/usr/bin/env python3
"""Re-export leads from leads_master.csv with updated filters + GPT V10.4 features."""
import pandas as pd
import sys
sys.path.insert(0, '/Users/dev/Documents/germany/lead_intel_v2')

from src.processors.exporter import Exporter
from src.processors.sce_scorer import SCEScorer
from src.processors.quality_reporter import QualityReporter
from src.processors.lead_role_classifier import LeadRoleClassifier
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    # Load leads_master.csv
    master_path = '/Users/dev/Documents/germany/lead_intel_v2/data/processed/leads_master.csv'
    df = pd.read_csv(master_path)
    logger.info(f"Loaded {len(df)} leads from leads_master.csv")
    
    # Check North Africa before export
    na_countries = ['Egypt', 'Morocco', 'Tunisia', 'Algeria', 'Libya']
    na_before = df[df['country'].isin(na_countries)]
    logger.info(f"North Africa leads before export: {len(na_before)}")
    
    # Convert to dict for processing
    leads = df.to_dict('records')
    
    # === GPT V10.4: Apply SCE Scoring ===
    logger.info("\n=== APPLYING SCE SCORING ===")
    sce_scorer = SCEScorer()
    leads, sce_stats = sce_scorer.score_batch(leads)
    logger.info(f"SCE: {sce_stats['sales_ready']} sales-ready, "
               f"{sce_stats['high_confidence']} high confidence")
    
    # === GPT V10.4: Re-classify roles with updated classifier ===
    logger.info("\n=== RE-CLASSIFYING ROLES ===")
    classifier = LeadRoleClassifier()
    customers, intermediaries, brands, unknown = classifier.classify_leads(leads)
    logger.info(f"Roles: CUSTOMER={len(customers)}, INTERMEDIARY={len(intermediaries)}, "
               f"BRAND={len(brands)}, UNKNOWN={len(unknown)}")
    
    # Merge back (brands are excluded from main output)
    all_leads = customers + intermediaries + unknown
    logger.info(f"Excluding {len(brands)} BRAND leads from export")
    
    # Export main list
    exporter = Exporter()
    result = exporter.export_targets(all_leads)
    
    # Export SCE sales-ready separately
    sales_ready = [l for l in all_leads if l.get('sce_sales_ready')]
    if sales_ready:
        exporter.export_targets(sales_ready, tag="_sce_sales_ready")
        logger.info(f"Exported {len(sales_ready)} SCE sales-ready leads")
    
    # === GPT V10.4: Generate Quality Report ===
    reporter = QualityReporter()
    report = reporter.generate_report(all_leads, sample_size=50, run_name="v104_reexport")
    
    # Check result
    if result:
        result_df = pd.read_csv(result)
        logger.info(f"\n=== EXPORT COMPLETE ===")
        logger.info(f"Total exported: {len(result_df)}")
        
        # Country breakdown
        logger.info(f"\nBy Country:")
        print(result_df['country'].value_counts().head(20).to_string())
        
        # North Africa check
        na_after = result_df[result_df['country'].isin(na_countries)]
        logger.info(f"\n✅ North Africa in final output: {len(na_after)}")
        if len(na_after) > 0:
            print(na_after['country'].value_counts().to_string())
        
        # SCE stats
        if 'sce_sales_ready' in result_df.columns:
            sce_ready = result_df[result_df['sce_sales_ready'] == True]
            logger.info(f"\n📊 SCE Sales Ready in output: {len(sce_ready)}")

if __name__ == "__main__":
    main()
